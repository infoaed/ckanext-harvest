from ckan.lib.base import _
import ckan.new_authz

def harvest_source_delete(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not ckan.new_authz.is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to delete harvest sources') % str(user)}
    else:
        return {'success': True}


