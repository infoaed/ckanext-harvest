from ckan.logic.validators import package_id_exists
from ckan.logic.converters import convert_to_extras

from ckan.lib.navl.validators import (ignore_missing,
                                      not_empty,
                                      empty,
                                      ignore,
                                      not_missing
                                      )

from ckanext.harvest.logic.validators import (harvest_source_url_validator,
                                              harvest_source_type_exists,
                                              harvest_source_config_validator,
                                              harvest_source_active_validator,
                                              harvest_source_type_exists,
                                              harvest_source_config_validator,
                                              harvest_source_extra_validator,
                                              harvest_source_frequency_exists,
                                              harvest_source_convert_from_config,
                                              harvest_source_id_exists,
                                              harvest_job_exists,
                                              harvest_object_extras_validator,
                                              )

def default_harvest_source_schema():

    schema = {
        'id': [ignore_missing, unicode, harvest_source_id_exists],
        'url': [not_empty, unicode, harvest_source_url_validator],
        'type': [not_empty, unicode, harvest_source_type_exists],
        'title': [ignore_missing,unicode],
        'description': [ignore_missing,unicode],
        'active': [ignore_missing,harvest_source_active_validator],
        'user_id': [ignore_missing,unicode],
        'config': [ignore_missing,harvest_source_config_validator],
        'publisher_id': [not_empty,unicode],
        'frequency': [ignore_missing, unicode, harvest_source_frequency_exists, convert_to_extras],
    }

    return schema


def harvest_source_form_schema():

    schema = default_harvest_source_schema()
    schema['save'] = [ignore]

    return schema

def harvest_object_create_schema():
    schema = {
        'guid': [ignore_missing, unicode],
        'content': [ignore_missing, unicode],
        'state': [ignore_missing, unicode],
        'job_id': [harvest_job_exists],
        'source_id': [ignore_missing, harvest_source_id_exists],
        'package_id': [ignore_missing, package_id_exists],
        'extras': [ignore_missing, harvest_object_extras_validator],
    }
    return schema

