from ckan.lib.base import _
from ckan.logic import NotFound
from ckan.model import User
import ckan.new_authz
from ckan.plugins import toolkit as pt

from ckanext.harvest.model import HarvestSource
from ckanext.harvest.logic.auth import get_job_object, get_obj_object


def auth_allow_anonymous_access(auth_function):
    '''
        Local version of the auth_allow_anonymous_access decorator that only
        calls the actual toolkit decorator if the CKAN version supports it
    '''
    if pt.check_ckan_version(min_version='2.2'):
        auth_function = pt.auth_allow_anonymous_access(auth_function)

    return auth_function


@auth_allow_anonymous_access
def harvest_source_show(context, data_dict):
    '''
        Authorization check for getting the details of a harvest source
    '''
    # Public
    return {'success': True}


@auth_allow_anonymous_access
def harvest_source_list(context, data_dict):
    '''
        Authorization check for getting a list of harvest sources

        Everybody can do it
    '''
    return {'success': True}


def harvest_job_show(context,data_dict):
    model = context['model']
    user = context.get('user')

    job = get_job_object(context,data_dict)

    if not user:
        return {'success': False, 'msg': _('Non-logged in users are not authorized to see harvest jobs')}

    if ckan.new_authz.is_sysadmin(user):
        return {'success': True}

    user_obj = User.get(user)
    if not user_obj or not job.source.publisher_id in [g.id for g in user_obj.get_groups(u'organization')]:
        return {'success': False, 'msg': _('User %s not authorized to read harvest job %s') % (str(user),job.id)}
    else:
        return {'success': True}

def harvest_job_list(context,data_dict):
    model = context['model']
    user = context.get('user')

    # Check user is logged in
    if not user:
        return {'success': False, 'msg': _('Only logged users are authorized to see their sources')}

    user_obj = User.get(user)

    # Checks for non sysadmin users
    if not ckan.new_authz.is_sysadmin(user):
        if not user_obj or len(user_obj.get_groups(u'organization')) == 0:
            return {'success': False, 'msg': _('User %s must belong to a publisher to list harvest jobs') % str(user)}

        source_id = data_dict.get('source_id',False)
        if not source_id:
            return {'success': False, 'msg': _('Only sysadmins can list all harvest jobs') % str(user)}

        source = HarvestSource.get(source_id)
        if not source:
            raise NotFound

        if not source.publisher_id in [g.id for g in user_obj.get_groups(u'organization')]:
            return {'success': False, 'msg': _('User %s not authorized to list jobs from source %s') % (str(user),source.id)}

    return {'success': True}

def harvest_object_show(context,data_dict):
    model = context['model']
    user = context.get('user')

    obj = get_obj_object(context,data_dict)

    if context.get('ignore_auth', False):
        return {'success': True}

    if not user:
        return {'success': False, 'msg': _('Non-logged in users are not authorized to see harvest objects')}

    if ckan.new_authz.is_sysadmin(user):
        return {'success': True}

    user_obj = User.get(user)
    if not user_obj or not obj.source.publisher_id in [g.id for g in user_obj.get_groups(u'organization')]:
        return {'success': False, 'msg': _('User %s not authorized to read harvest object %s') % (str(user),obj.id)}
    else:
        return {'success': True}

def harvest_object_list(context,data_dict):
    model = context['model']
    user = context.get('user')

    # Check user is logged in
    if not user:
        return {'success': False, 'msg': _('Only logged users are authorized to see their sources')}

    user_obj = User.get(user)

    # Checks for non sysadmin users
    if not ckan.new_authz.is_sysadmin(user):
        if not user_obj or len(user_obj.get_groups(u'organization')) == 0:
            return {'success': False, 'msg': _('User %s must belong to a publisher to list harvest objects') % str(user)}

        source_id = data_dict.get('source_id',False)
        if not source_id:
            return {'success': False, 'msg': _('Only sysadmins can list all harvest objects') % str(user)}

        source = HarvestSource.get(source_id)
        if not source:
            raise NotFound

        if not source.publisher_id in [g.id for g in user_obj.get_groups(u'organization')]:
            return {'success': False, 'msg': _('User %s not authorized to list objects from source %s') % (str(user),source.id)}

    return {'success': True}


@auth_allow_anonymous_access
def harvesters_info_show(context,data_dict):
    # Public
    return {'success': True}
