from ckan.lib.base import _
import ckan.new_authz

def harvest_source_create(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not ckan.new_authz.is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to create harvest sources') % str(user)}
    else:
        return {'success': True}

def harvest_job_create(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not ckan.new_authz.is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to create harvest jobs') % str(user)}
    else:
        return {'success': True}

def harvest_job_create_all(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not ckan.new_authz.is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to create harvest jobs for all sources') % str(user)}
    else:
        return {'success': True}

