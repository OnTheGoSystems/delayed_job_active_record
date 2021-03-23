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
    Delayed::Job.delete_all
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
        processing_order << [job.fair_id, jobs.key(job)].join
        job.delete
      end
    end

    context 'when no ranks calculated' do
      it 'processes using simple order' do
        jobs.keys.size.times { process_next_job! }
        expect(processing_order).to eq ["A3", "B5", "C6", "E9", "E10", "E11", "E12"]
      end
    end

    context 'when ranks calculated' do
      let(:expected_ranks) do
        [
          { fair_id: 'A', busy: 2, waiting: 1, rank: -1 },
          { fair_id: 'B', busy: 1, waiting: 1, rank: 0 },
          { fair_id: 'C', busy: 0, waiting: 1, rank: 1 },
          { fair_id: 'D', busy: 2, waiting: 0, rank: -2 },
          { fair_id: 'E', busy: 0, waiting: 4, rank: 1 },
        ]
      end

      it 'returns ranks' do
        ranks = described_class.recalculate_ranks!
        expect(ranks).to eq(expected_ranks)

        fetched_ranks = described_class.fetch_ranks
        expect(fetched_ranks).to eq(expected_ranks)
      end

      it 'uses ranks for job election' do
        jobs.keys.size.times do
          process_next_job!
          described_class.recalculate_ranks!
        end

        expect(processing_order).to eq ["C6", "E9", "E10", "E11", "E12", "B5", "A3"]
      end
    end
  end
end
