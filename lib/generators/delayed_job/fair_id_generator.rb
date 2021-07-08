# frozen_string_literal: true

require "generators/delayed_job/delayed_job_generator"
require "generators/delayed_job/next_migration_version"
require "rails/generators/migration"
require "rails/generators/active_record"
require_relative './active_record_generator'

# Extend the DelayedJobGenerator so that it creates an AR migration
module DelayedJob
  class FairIdGenerator < ActiveRecordGenerator
    def create_migration_file
      migration_template(
        "add_fair_id_migration.rb.tmpl",
        "db/migrate/add_fair_id_to_delayed_jobs.rb",
        migration_version: migration_version
      )
    end

    def create_executable_file; end
  end
end
