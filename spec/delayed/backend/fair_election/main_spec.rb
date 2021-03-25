# frozen_string_literal: true

require "helper"
require "delayed/backend/active_record"


describe Delayed::Backend::ActiveRecord::FairSql::Service do
  let(:dbms) { "MySQL" }
  let(:worker) { instance_double(Delayed::Worker, name: "worker-X", read_ahead: 1) }

  before do
    allow(Delayed::Backend::ActiveRecord::Job.connection).to receive(:adapter_name).at_least(:once).and_return(dbms)
    Delayed::Backend::ActiveRecord.configuration.reserve_sql_strategy = :fair_sql
    allow(described_class).to receive(:rand_func) { '' }
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
          { fair_id: 'A', busy: 2, waiting: 1, rank: -2 },
          { fair_id: 'B', busy: 1, waiting: 1, rank: -1 },
          { fair_id: 'C', busy: 0, waiting: 1, rank: 1 },
          { fair_id: 'D', busy: 2, waiting: 0, rank: -3 },
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
        described_class.recalculate_ranks!

        jobs.keys.size.times do
          process_next_job!
          described_class.recalculate_ranks!
        end

        expect(processing_order).to eq ["C6", "E9", "E10", "E11", "E12", "B5", "A3"]
      end

      it 'cleans old ranks' do
        described_class.recalculate_ranks!

        2.times do
          process_next_job!
          sleep 1.5
          described_class.recalculate_ranks!
        end

        ranks_before_cleaning = described_class.rank_klass.count
        described_class.clean_ranks
        ranks_after_cleaning = described_class.rank_klass.count

        expect(ranks_after_cleaning < ranks_before_cleaning).to be_truthy
      end

      it 'when ranks missing' do
        described_class.recalculate_ranks!

        jobs.keys.size.times do
          process_next_job!
        end

        expect(processing_order).to eq ["C6", "E9", "E10", "E11", "E12", "B5", "A3"]
      end

      it 'uses respects order of jobs when new jobs created, and some jobs processed' do
        described_class.recalculate_ranks!

        jobs.keys.size.times do |index|
          process_next_job!

          if index == 2
            jobs[jobs.size + 1] = 1.delay(fair_id: 'F').to_s
            jobs[jobs.size + 1] = 1.delay(fair_id: 'F').to_s
          end

          if index == 5
            jobs[1].delete
            jobs[2].delete
          end

          described_class.recalculate_ranks!
        end

        expect(processing_order).to eq ["C6", "E9", "E10", "E11", "E12", "F13", "A3", "F14", "B5"]
      end
    end
  end
end
