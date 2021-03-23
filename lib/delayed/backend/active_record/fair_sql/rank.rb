module Delayed
  module Backend
    module ActiveRecord
      module FairSql
        class Rank < ::ActiveRecord::Base
          self.table_name = 'delayed_jobs_fair_ranks'

          NEGATIVE_RANK = -10**10
          SYSTEM_RECORD = 'DJ_SYSTEM_PRIMARY'.freeze

          scope :current_ranks, ->{ where(timestamp: current_timestamp!).where.not(fair_id: SYSTEM_RECORD) }

          class << self
            def system_record!
              where(fair_id: SYSTEM_RECORD, rank: NEGATIVE_RANK).order(timestamp: :desc).first_or_create!
            end

            def current_timestamp!
              system_record!.timestamp
            end

            def update_current_timestamp!(timestamp)
              system_record!.update!(timestamp: timestamp)
            end
          end
        end
      end
    end
  end
end
