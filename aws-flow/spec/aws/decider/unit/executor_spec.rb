##
# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
##

require_relative 'setup'

describe ForkingExecutor do

  it "makes sure that forking executors basic execute works" do
    test_file_name = "ForkingExecutorTestFile"
    begin
      forking_executor = ForkingExecutor.new
      File.exists?(test_file_name).should == false
      forking_executor.execute do
        File.new(test_file_name, 'w')
      end
      sleep 3
      File.exists?(test_file_name).should == true
    ensure
      File.unlink(test_file_name)
    end
  end

  it "ensures that one worker for forking executor will only allow one thing to be processed at a time" do
    executor = ForkingExecutor.new(:max_workers => 1)

    test_file_name = "ForkingExecutorRunOne"
    File.new(test_file_name, "w")
    start_time = Time.now
    executor.execute do
      File.open(test_file_name, "a+") { |f| f.write("First Execution\n")}
      sleep 4
    end
    # Because execute will block if the worker queue is full, we will wait here
    # if we have reached the max number of workers
    executor.execute { 2 + 2 }
    finish_time = Time.now
    # If we waited for the first task to finish, then we will have waited at
    # least 4 seconds; if we didn't, we should not have waited. Thus, if we have
    # waited > 3 seconds, we have likely waited for the first task to finish
    # before doing the second one
    (finish_time - start_time).should > 3
    File.unlink(test_file_name)
  end

  it "ensures that you cannot execute more tasks on a shutdown executor" do
    forking_executor = ForkingExecutor.new
    forking_executor.execute {}
    forking_executor.execute {}
    forking_executor.shutdown(1)
    expect { forking_executor.execute { "yay" } }.to raise_error RejectedExecutionException
  end

  context "#remove_completed_pids" do
    class Status
      def success?; true; end
    end

    context "with block=false" do
      it "should not block if not child process available" do

        # stub out Process.waitpid2 to return only 2 processes
        allow(Process).to receive(:waitpid2).and_return(nil)
        allow_any_instance_of(AWS::Flow::ForkingExecutor).to receive(:fork).and_return(1, 2, 3)
        executor = ForkingExecutor.new(max_workers: 3)

        executor.execute { sleep 1 }
        executor.execute { sleep 1 }
        executor.execute { sleep 1 }
        executor.pids.size.should == 3
        executor.send(:remove_completed_pids, false)
        # The two processes that are completed will be reaped.
        executor.pids.size.should == 3
      end

      it "should reap all completed child processes" do

        allow(Process).to receive(:waitpid2).and_return([1, Status.new], [2, Status.new], [3, Status.new] )
        allow_any_instance_of(AWS::Flow::ForkingExecutor).to receive(:fork).and_return(1, 2, 3)
        executor = ForkingExecutor.new(max_workers: 3)

        executor.execute { sleep 1 }
        executor.execute { sleep 1 }
        executor.execute { sleep 1 }
        executor.pids.size.should == 3
        executor.send(:remove_completed_pids, false)
        # The two processes that are completed will be reaped.
        executor.pids.size.should == 0
      end
    end
    context "with block=true" do
      it "should wait for at least one child process to become available and then reap as many as possible" do

        # stub out Process.waitpid2 to return only 2 processes
        allow(Process).to receive(:waitpid2).and_return([1, Status.new], [2, Status.new], nil)
        allow_any_instance_of(AWS::Flow::ForkingExecutor).to receive(:fork).and_return(1, 2, 3)
        executor = ForkingExecutor.new(max_workers: 3)

        executor.execute { sleep 1 }
        executor.execute { sleep 1 }
        executor.execute { sleep 5 }

        executor.pids.size.should == 3

        executor.send(:remove_completed_pids, true)
        # It will wait for one of the processes to become available and then
        # reap as many as possible
        executor.pids.size.should == 1
      end
    end
  end
end

describe ThreadingExecutor do

  before (:each) do
    logger_mock = double("logger").as_null_object
    random_mock = double("random")
    random_mock.stub(:rand).and_return(987654321)

    allow(Utilities::LogFactory).to receive(:make_logger).and_return(logger_mock)
    allow(Time).to receive(:now).and_return(123456)
    allow(Random).to receive(:new).and_return(random_mock)
  end

  def test_model(options={})
    AWS::Flow::ThreadingExecutor.new(options)
  end

  context "#execute" do

    it "should create a new thread and add the thread into the thread pool" do
      t_model = test_model
      mock_thread = double("thread")
      expect(Thread).to receive(:new).and_return(mock_thread)
      t_model.execute()
      expect(t_model.threads).to eq([ mock_thread ])
    end

    it "should call the given block inside the thread" do
      t_model = test_model
      blk = lambda{}

      expect(blk).to receive(:call)
      t_model.execute(&blk)
    end
  end

  context "#shutdown" do

    def running_thread
      double("running", :status => "run")
    end

    def completed_thread
      double("completed", :status => false)
    end

    def failed_thread
      double("failed", :status => nil)
    end

    it "should try for exactly n tries to terminate gracefully if given a timeout and the list of threads is greater than the timeout" do
      thread_pool = [ running_thread, running_thread, completed_thread, failed_thread ]
      t_model = test_model
      t_model.threads = thread_pool

      expect(Kernel).to receive(:sleep).twice
      t_model.shutdown(2)
    end
  end
end
