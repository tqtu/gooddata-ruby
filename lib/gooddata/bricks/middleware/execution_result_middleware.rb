# encoding: UTF-8
# Copyright (c) 2019, GoodData Corporation. All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

module GoodData
  module Bricks
    module ExecutionStatus
      OK = 'OK'
      ERROR = 'ERROR'
      WARNING = 'WARNING'
    end

    class ExecutionResultMiddleware < Bricks::Middleware

      def call(params)
        self.class.update_execution_result(params, params["EXECUTION_RESULT_STATUS"], params["EXECUTION_RESULT_MESSAGE"])
        @app.call(params)
      end

      def self.update_execution_result(params, status, message="")
        if (status != ExecutionStatus::OK && status != ExecutionStatus::ERROR && status != ExecutionStatus::ERROR)
          GoodData.logger.warn("Unknown execution status #{status}, ignored it.")
        end

        result = {
          executionResult: {
            status: status,
            message: message
          }
        }
        update_result(params, result)
      end

      private

      def self.update_result(params, result)
        begin
          execution_result_logger_file = params['GDC_EXECUTION_RESULT_LOG_PATH'] || ENV['GDC_EXECUTION_RESULT_LOG_PATH']
          if execution_result_logger_file.nil?
            return
          end

          File.open(execution_result_logger_file, 'w') { |file| file.write(JSON.pretty_generate(result)) }
          params["EXECUTION_RESULT_COMPLETED"] = true

        rescue Exception => e # rubocop:disable RescueException
          params["EXECUTION_RESULT_COMPLETED"] = false
          GoodData.logger.warn("Cannot write execution result, reason: #{e.message}") unless GoodData.logger.nil?
        end
      end

    end
  end
end
