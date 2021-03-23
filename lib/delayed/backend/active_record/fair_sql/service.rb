# frozen_string_literal: true

module Delayed
  module Backend
    module ActiveRecord
      module FairSql
        class Service
          class << self
            def reserve(ready_scope, worker, now)
              raise ArgumentError, ":fair_sql is allowed only for MySQL" unless job_klass.connection.adapter_name.downcase.include?('mysql')
              job_klass.reserve_with_scope_using_optimized_sql(ready_scope, worker, now)
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
