module Delayed
  module Backend
    module ActiveRecord
      module FairSql
        class Rank < ::ActiveRecord::Base
          self.table_name = 'delayed_jobs_fair_ranks'
        end
      end
    end
  end
end