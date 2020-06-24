# (C) 2019-2020 GoodData Corporation
require_relative 'base_action'

module GoodData
  module LCM2
    class SynchronizeDateDimension < BaseAction
      DESCRIPTION = 'Synchronize Date Dimension'
      DATE_DIMENSION_CUSTOM_V2 = 'custom_v2'
      DATE_DIMENSION_OLD = %w[gooddata custom]

      PARAMS = define_params(self) do
        description 'Client Used for Connecting to GD'
        param :gdc_gd_client, instance_of(Type::GdClientType), required: true

        description 'Specifies how to synchronize LDM and resolve possible conflicts'
        param :synchronize_ldm, instance_of(Type::SynchronizeLDM), required: false, default: 'diff_against_master_with_fallback'

        description 'Synchronization Info'
        param :synchronize, array_of(instance_of(Type::SynchronizationInfoType)), required: true, generated: true

        description 'List upgrade datasets'
        param :datasets, array_of(instance_of(Type::StringType)), required: false, default: nil
      end

      RESULT_HEADER = [
          :from,
          :to,
          :status
      ]

      class << self
        def call(params)
          results = []
          params.synchronize.map do |segment_info|
            result = synchronize_date_dimension(params, segment_info)
            results.concat(result)
          end

          {
              results: results
          }
        end

        def synchronize_date_dimension(params, segment_info)
          results = []
          client = params.gdc_gd_client
          diff_against_master = %w(diff_against_master).include?(params[:synchronize_ldm].downcase)
          latest_blueprint = segment_info[:latest_master]&.blueprint(include_ca: true)
          previous_blueprint = segment_info[:previous_master]&.blueprint(include_ca: true)
          is_all = !params.datasets || params.datasets.empty?
          is_upgrade = true
          is_upgrade = blueprint_upgrade(latest_blueprint, previous_blueprint, is_all, params.datasets) if diff_against_master
          if (is_upgrade)
            segment_info[:to].pmap do |entry|
              pid = entry[:pid]
              to_project = client&.projects(pid) || fail("Invalid 'to' project specified - '#{pid}'")
              to_blueprint = to_project&.blueprint(include_ca: true)
              is_upgrade, upgrade_datasets = blueprint_upgrade(latest_blueprint, to_blueprint, is_all, params.datasets)
              next unless is_upgrade
              message = get_upgrade_message(is_all, upgrade_datasets)
              results << {
                  from: segment_info[:from],
                  to: pid,
                  status: to_project&.upgrade_custom_v2(message)
              }
            end
          end

          results
        end

        def blueprint_upgrade(src_blueprint, dest_blueprint, is_all, include_datasets)
          dest_dates = get_date_dimensions(dest_blueprint) if dest_blueprint
          upgrade_datasets = []
          dest_dates&.each do |dest|
            src_dim = get_date_dimension(src_blueprint, dest[:id])
            identifier = include_datasets.find {|dataset| dataset.start_with?(src_dim[:identifier_prefix]) } if include_datasets && !include_datasets.empty?
            next unless src_dim && (!include_datasets || include_datasets.empty? || identifier)
            upgrade_datasets << src_dim[:id] if upgrade?(src_dim, dest)

            if upgrade?(src_dim, dest)
              is_all ?
                  upgrade_datasets << src_dim[:id]
                  :
                  upgrade_datasets << identifier
            end
          end

          [!upgrade_datasets.empty?, upgrade_datasets]
        end

        def get_upgrade_message(is_all, upgrade_datasets)
          is_all ?
              {
                  upgrade: {
                      dateDatasets: {
                          upgrade: "all",
                      }
                  }
              }
              :
              {
                  upgrade: {
                      dateDatasets: {
                          upgrade: "exact",
                          datasets: upgrade_datasets
                      }
                  }
              }
        end

        def upgrade?(src_dim, dest_dim)
          src_dim[:urn]&.include?(DATE_DIMENSION_CUSTOM_V2) && !dest_dim[:urn]&.include?(DATE_DIMENSION_CUSTOM_V2) && DATE_DIMENSION_OLD.any? { |e| dest_dim[:urn]&.include?(e) }
        end

        def get_date_dimension(blueprint, id)
          return GoodData::Model::ProjectBlueprint.find_date_dimension(blueprint, id)
        rescue StandardError => e
          return nil
        end

        def get_date_dimensions(blueprint)
          return GoodData::Model::ProjectBlueprint.date_dimensions(blueprint)
        end
      end
    end
  end
end
