# frozen_string_literal: true

require "helper"
require "delayed/backend/active_record"


describe Delayed::Backend::ActiveRecord::FairSql::Service do
  let(:dbms) { "MySQL" }
  let(:worker) { instance_double(Delayed::Worker, name: "worker-X", read_ahead: 1) }

  before do
    allow(Delayed::Backend::ActiveRecord::Job.connection).to receive(:adapter_name).at_least(:once).and_return(dbms)
    Delayed::Backend::ActiveRecord.configuration.reserve_sql_strategy = :fair_sql
  end

  let!(:jobs) do
    n = 1
    {
      1 => n.delay(fair_id: 'A', locked_at: 5.minutes.ago).to_s,
      2 => n.delay(fair_id: 'A', locked_at: 5.minutes.ago).to_s,
      3 => n.delay(fair_id: 'A').to_s,

      4 => n.delay(fair_id: 'B', locked_at: 5.minutes.ago).to_s,
      5 => n.delay(fair_id: 'B').to_s,

      6 => n.delay(fair_id: 'C').to_s,

      7 => n.delay(fair_id: 'D', locked_at: 5.minutes.ago).to_s,
      8 => n.delay(fair_id: 'D', locked_at: 5.minutes.ago).to_s,

      9 => n.delay(fair_id: 'E').to_s,
      10 => n.delay(fair_id: 'E').to_s,
      11 => n.delay(fair_id: 'E').to_s,
      12 => n.delay(fair_id: 'E').to_s,
    }
  end

  describe 'process Jobs' do
    let(:processing_order) { [] }

    def process_next_job!
      job = Delayed::Backend::ActiveRecord::Job.reserve(worker)
      if job
        processing_order << jobs.key(job)
        job.delete
      end
    end

    before do
      jobs.keys.size.times { process_next_job! }
    end

    it do
      expect(processing_order).to eq [3, 5, 6, 9, 10, 11, 12]
    end
  end
end
