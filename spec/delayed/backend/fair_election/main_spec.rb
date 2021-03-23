# frozen_string_literal: true

require "helper"
require "delayed/backend/active_record"


describe Delayed::Backend::ActiveRecord::FairSql::Service do
  describe "behavior" do
    let(:relation_class) { Delayed::Job.limit(1).class }
    let(:worker) { instance_double(Delayed::Worker, name: "worker01", read_ahead: 1) }
    let(:limit) { instance_double(relation_class, update_all: 0) }
    let(:where) { instance_double(relation_class, update_all: 0) }
    let(:scope) { instance_double(relation_class, limit: limit, where: where) }
    let(:job) { instance_double(Delayed::Job, id: 1) }

    before do
      allow(Delayed::Backend::ActiveRecord::Job.connection).to receive(:adapter_name).at_least(:once).and_return(dbms)
      Delayed::Backend::ActiveRecord.configuration.reserve_sql_strategy = reserve_sql_strategy
    end

    context "with reserve_sql_strategy option set to :fair_sql" do
      let(:dbms) { "MySQL" }
      let(:reserve_sql_strategy) { :fair_sql }

      it "uses the fair sql version" do
        allow(Delayed::Backend::ActiveRecord::Job).to receive(:reserve_with_scope_using_optimized_sql)
        Delayed::Backend::ActiveRecord::Job.reserve_with_scope(scope, worker, Time.current)
        expect(Delayed::Backend::ActiveRecord::Job).to have_received(:reserve_with_scope_using_optimized_sql).once
      end
    end
  end
end
