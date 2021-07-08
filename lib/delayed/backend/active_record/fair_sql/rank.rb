module Delayed
  module Backend
    module ActiveRecord
      module FairSql
        class Rank < ::ActiveRecord::Base
          self.table_name = 'delayed_jobs_fair_ranks'

          NEGATIVE_RANK = -10**6
          SYSTEM_RECORD = 'DJ_SYSTEM_PRIMARY'.freeze

          scope :not_system, -> { where.not(fair_id: SYSTEM_RECORD) }
          scope :current_ranks, ->{ not_system.where(timestamp: current_timestamp!) }

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

            def clean_outdated!
              where('timestamp < ?', current_timestamp!).delete_all
            end
          end
        end
      end
    end
  end
end
