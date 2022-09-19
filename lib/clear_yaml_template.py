import yaml


def clear_yaml_template(path_to_source_file, path_to_result_file):

    template_file = yaml.safe_load(open(path_to_source_file))
    yaml.dump(template_file, open(path_to_result_file, 'w'))
