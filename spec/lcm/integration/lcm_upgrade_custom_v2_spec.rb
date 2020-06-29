require_relative 'support/constants'
require_relative 'support/configuration_helper'
require_relative 'support/lcm_helper'
require_relative 'brick_runner'
require_relative 'shared_contexts_for_lcm'

# global variables to simplify passing stuff between shared contexts and examples
$master_projects = []
$client_projects = []
$master = false

output_stage_prefix = GoodData::Environment::VCR_ON ? nil : Support::OUTPUT_STAGE_PREFIX

schedule_additional_hidden_params = {
  hidden_msg_from_release_brick: 'Hi, I was set by a brick but keep it secret',
  SECURE_PARAM_2: 'I AM SET TOO'
}

process_additional_hidden_params = {
  process: {
    component: {
      configLocation: {
        s3: {
          path: 's3://s3_bucket/s3_folder/',
          accessKey: 's3_access_key',
          secretKey: 's3_secret_key',
          serverSideEncryption: true
        }
      }
    }
  }
}

describe 'the whole life-cycle', :vcr do
  include_context 'lcm bricks',
                  schedule_additional_hidden_params: schedule_additional_hidden_params,
                  process_additional_hidden_params: process_additional_hidden_params

  describe '1 - Initial Release' do
    before(:all) do

      $master_projects = BrickRunner.release_brick context: @test_context, template_path: '../params/release_brick.json.erb', client: @prod_rest_client

      # $client_projects = BrickRunner.provisioning_brick context: @test_context, template_path: '../params/provisioning_brick.json.erb', client: @prod_rest_client
      #
      # $client_projects = BrickRunner.rollout_brick context: @test_context, template_path: '../params/rollout_brick.json.erb', client: @prod_rest_client
      #
      $master = $master_projects.first

      puts "DEBUG tuqt: master again: $master:#{$master}"
      blueprint = GoodData::Model::ProjectBlueprint.new($master.blueprint)
      puts "DEBUG tuqt: master again: $master blueprint:#{blueprint}"
      puts "DEBUG tuqt: master again: $master blueprint to_wire:#{blueprint.to_wire}"


      # $master_projects = BrickRunner.release_brick context: @test_context, template_path: '../params/release_brick.json.erb', client: @prod_rest_client
      #
      # $client_projects = BrickRunner.rollout_brick context: @test_context, template_path: '../params/rollout_brick_include_deprecated.json.erb', client: @prod_rest_client

    end

    it 'migrates LDM' do
      $client_projects.each do |target_project|
        blueprint = GoodData::Model::ProjectBlueprint.new($master.blueprint)
        diff = Support::ComparisonHelper.compare_ldm(blueprint, target_project.pid, @prod_rest_client)
        if target_project.pid == $client_project.pid
          expect(diff['updateOperations'].empty?).to eq(false)
        else
          expect(diff['updateOperations']).to eq([])
          expect(diff['updateScripts']).to eq([])
        end
      end
    end
  end
end




