#  Copied from https://github.com/socrata/bellerophon/blob/master/scripts/list_four_by_four.rb
#  with some slight modifications so it doesn't require rails and outputs what we need

require 'httparty'
require 'parallel'
require 'active_support'
require 'active_support/core_ext'

bellerophon_domain = ARGV[0] || 'staging-bellerophon.herokuapp.com'
output_file = ARGV[1] || 'bellerophon_fxfs'
missing_env_variables = %w(SODA_AUTH_USERNAME SODA_AUTH_PASSWORD SODA_AUTH_TOKEN).find_all { |var| ENV[var].blank? }
raise "Please set the env variables: #{missing_env_variables.join(', ')}" unless missing_env_variables.empty?

all_datasets = []
dataset_keys = {
    'ngBudget' => {domain_key: 'dataset_domain', dataset_keys: %w(operating_budget_dataset_id revenue_budget_dataset_id capital_budget_dataset_id capital_projects_dataset_id phase_dataset_id)},
    'spending' => {domain_key: 'dataset_domain', dataset_keys: %w(vendor_dataset_id ledger_dataset_id glossary_dataset_id)},
    'payroll' => {domain_key: 'dataset_domain', dataset_keys: %w(dataset_id)},
    'budget' => {domain_key: 'dataset_domain', dataset_keys: %w(operating_budget_dataset_id capital_budget_dataset_id capital_projects_dataset_id phase_dataset_id)},
    'ngSpending' => {domain_key: 'dataset_domain', dataset_keys: %w(vendor_dataset_id ledger_dataset_id)},
    'insightDashboard' => {domain_key: 'dataset_domain', dataset_keys: %w(dataset_id)},
    'capitalProjectsExplorer' => {domain_key: 'dataset_domain', dataset_keys: %w(dataset_id shape_dataset_id financial_dataset_id stages_dataset_id district_shape_dataset_id)},
    'ngPayroll' => {domain_key: 'dataset_domain', dataset_keys: %w(payroll_dataset_id)},
    'citizenConnect' => {dataset_keys: %w(shape_datasets tickets_datasets places_datasets)}
}

def get_datasets_from_response(config, application, key_config)
  datasets = []
  domain = config[key_config[:domain_key]]
  app_urls = config.map { |k, v| v if k.match(/^customer_domain/) }.compact

  if application == 'citizenConnect'
    datasets = get_all_cc_datasets(config, key_config, app_urls)
  else
    config.slice(*key_config[:dataset_keys]).values.each do |dataset_four_x_four|
      datasets << [domain, dataset_four_x_four, app_urls]
    end
  end
  datasets
end

def get_all_cc_datasets(config, key_config, app_urls)
  datasets = []
  config.slice(*key_config[:dataset_keys]).values.each do |json_config|
    dataset_configs = JSON.parse(json_config)
    dataset_configs.each do |dataset_config|
      domain = dataset_config.map { |k, v| v if k.match(/_dataset_domain$/) }.compact
      dataset_four_x_four = dataset_config.map { |k, v| v if k.match(/_dataset_id$/) }.compact
      entry = [domain, dataset_four_x_four].flatten
      entry.push(app_urls)
      datasets << entry
    end
  end
  datasets
end

def get_datasets(bellerophon_domain, application, key_config)
  datasets = []
  keys_to_retrieve = (key_config[:dataset_keys] + [key_config[:domain_key]]).compact
  url = "http://#{bellerophon_domain}/#{application}/app_configurations/list.json?keys=#{keys_to_retrieve.join(',')}"
  response = HTTParty.get(url)
  raise "url #{url} failed. error. #{response.body} code: #{response.code}" unless response.ok?
  response.parsed_response.each do |config|
    datasets += get_datasets_from_response(config, application, key_config)
  end
  datasets
end

def resolves?(url)
  begin
    return HTTParty.get("http://#{url}").ok?
  rescue Exception
    return false
  end
end

dataset_keys.each do |application, app_keys_config|
  datasets = get_datasets(bellerophon_domain, application, app_keys_config)
  puts "#{datasets.size} datasets found for #{application}"
  Parallel.each(datasets, in_threads: 8) do |(domain, dataset, app_urls)|
    next if domain.blank? || dataset.blank?
    begin
      resolving_apps = app_urls.uniq.select{ |url| resolves?(url) }.join(',')
    rescue Exception => e
      puts e
    end
    all_datasets.push([dataset, domain, application, resolving_apps])
  end
end

CSV.open(output_file, 'wb', col_sep: "\t") do |tsv|
  all_datasets.each do |entry|
    tsv << entry
  end
end
