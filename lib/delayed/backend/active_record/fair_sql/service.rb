# frozen_string_literal: true

require_relative './rank'

module Delayed
  module Backend
    module ActiveRecord
      module FairSql
        class Service
          class << self
            RANK_SAVING_BATCH = 200
            JOIN_LIMIT = 100
            attr_accessor :queues

            def reserve(ready_scope, worker, now)
              raise ArgumentError, ":fair_sql is allowed only for MySQL" unless job_klass.connection.adapter_name.downcase.include?('mysql')
              scope = apply_ranks(ready_scope)
              job_klass.reserve_with_scope_using_optimized_sql(scope, worker, now)
            end

            def apply_ranks(scope)
              if queues.nil? || (self.queues & Worker.queues).size > 0
                top_ranks = "SELECT * FROM delayed_jobs_fair_ranks WHERE timestamp = #{rank_klass.current_timestamp!} ORDER BY delayed_jobs_fair_ranks.rank DESC LIMIT #{JOIN_LIMIT}"
                scope = scope.joins("LEFT JOIN (#{top_ranks}) AS ranks ON ranks.fair_id = delayed_jobs.fair_id")
                scope = scope.select(select_grouped).group(:fair_id).distinct
                scope = scope.reorder("delayed_jobs.priority ASC, ranks.rank DESC, #{rand_order}")
                scope
              else
                scope = scope.reorder("delayed_jobs.priority, #{rand_order}")
                scope
              end
            end

            def select_grouped
              columns = %w[delayed_jobs.id priority attempts handler last_error run_at locked_at failed_at locked_by queue delayed_jobs.fair_id]
              columns.map { |c| "ANY_VALUE(#{c}) as #{c.split('.').last}" }.join(',')
            end

            def recalculate_ranks!(timestamp = newest_timestamp)
              ranks = calculate_ranks(timestamp)

              ranks.each_slice(RANK_SAVING_BATCH).each do |g|
                rank_klass.create(g)
              end

              rank_klass.update_current_timestamp!(timestamp)
              ranks.map { |r| r.except(:timestamp) }
            end

            def calculate_ranks(timestamp = newest_timestamp)
              scope = job_klass
              scope = scope.where(queue: self.queues) if self.queues.present? && self.queues.size > 0

              stats = scope.where(last_error: nil).group(:fair_id).select(
                [
                  'fair_id',
                  'sum(case when locked_at IS NOT NULL then 1 else 0 end) as b',
                  'sum(case when locked_at IS NOT NULL then 0 else 1 end) as w'
                ].join(',')
              )

              stats.map do |st|
                { fair_id: st.fair_id, busy: st.b, waiting: st.w, rank: calc_rank(st.b, st.w), timestamp: timestamp }
              end
            end

            def calc_rank(busy, waiting)
              rank = (busy * -100)
              rank -= 1 if busy > 0 && waiting > 0
              rank
            end

            def rand_order
              'rand()'
            end

            def fetch_ranks
              rank_klass.current_ranks.map do |r|
                { fair_id: r.fair_id, busy: r.busy, waiting: r.waiting, rank: r.rank }
              end
            end

            def clean_ranks
              rank_klass.clean_outdated!
            end

            def newest_timestamp
              Time.now.utc.to_i
            end

            def rank_klass
              Delayed::Backend::ActiveRecord::FairSql::Rank
            end

            def job_klass
              Delayed::Backend::ActiveRecord::Job
            end
          end
        end
      end
    end
  end
end
