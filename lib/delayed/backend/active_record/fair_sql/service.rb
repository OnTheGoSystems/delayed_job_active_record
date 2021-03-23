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

            def reserve(ready_scope, worker, now)
              raise ArgumentError, ":fair_sql is allowed only for MySQL" unless job_klass.connection.adapter_name.downcase.include?('mysql')
              scope = ready_scope
              top_ranks = "SELECT * FROM delayed_jobs_fair_ranks ORDER BY rank DESC LIMIT #{JOIN_LIMIT}"
              scope = scope.joins("LEFT JOIN (#{top_ranks}) AS ranks ON ranks.fair_id = delayed_jobs.fair_id")
              scope = scope.reorder("rank DESC, priority ASC, run_at ASC")

              job_klass.reserve_with_scope_using_optimized_sql(scope, worker, now)
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
              stats = job_klass.group(:fair_id).select(
                [
                  'fair_id',
                  'sum(case when locked_at IS NOT NULL then 1 else 0 end) as b',
                  'sum(case when locked_at IS NOT NULL then 0 else 1 end) as w'
                ].join(',')
              )

              stats.map do |st|
                diff = st.w - st.b
                rank = diff <= 0 ? diff : 1
                { fair_id: st.fair_id, busy: st.b, waiting: st.w, rank: rank, timestamp: timestamp }
              end
            end

            def fetch_ranks
              rank_klass.current_ranks.map do |r|
                { fair_id: r.fair_id, busy: r.busy, waiting: r.waiting, rank: r.rank }
              end
            end

            def best_ranks
              fetch_ranks.sort_by { |r| r[:rank] }.reverse[0..JOIN_LIMIT].to_a
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
