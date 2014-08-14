import logging
import re
import uuid

from sqlalchemy.sql import update, bindparam

from ckan import logic
from ckan import model
from ckan.model import Session, Package
from ckan.lib import maintain
from ckan.logic import ValidationError, NotFound, get_action
from ckan.logic.schema import default_create_package_schema
from ckan.lib.navl.validators import ignore_missing, ignore, not_empty
from ckan.lib.munge import munge_title_to_name, substitute_ascii_equivalents

from ckanext.harvest.model import HarvestObject, HarvestGatherError, \
                                    HarvestObjectError

from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckan.lib.helpers import json

log = logging.getLogger(__name__)

def munge_tag(tag):
    '''Replaces/removes any characters that are not in the standard CKAN tag
    schema.'''
    tag = substitute_ascii_equivalents(tag)
    tag = tag.lower().strip()
    return re.sub(r'[^a-zA-Z0-9 -]', '', tag).replace(' ', '-')

def munge_tags(package_dict):
    tags = package_dict.get('tags', [])
    tags = [munge_tag(t) for t in tags]
    tags = list(set(tags))
    package_dict['tags'] = tags


def remove_duplicates_in_a_list(list_):
    seen = set()
    seen_add = seen.add
    return [x for x in list_ if not (x in seen or seen_add(x))]


class HarvesterBase(SingletonPlugin):
    '''
    Generic class for harvesters, providing them with a common import_stage and
    various helper functions.
    '''
    implements(IHarvester)

    config = None

    @staticmethod
    def _gen_new_name(title):
        '''
        Creates a URL friendly name from a title
        '''
        name = munge_title_to_name(title).replace('_', '-')
        while '--' in name:
            name = name.replace('--', '-')
        return name

    @staticmethod
    def _check_name(name):
        '''
        Checks if a package name already exists in the database, and adds
        a counter at the end if it does exist.
        '''
        like_q = u'%s%%' % name
        pkg_query = Session.query(Package).filter(Package.name.ilike(like_q)).limit(100)
        taken = [pkg.name for pkg in pkg_query]
        if name not in taken:
            return name
        else:
            counter = 1
            while counter < 101:
                if name+str(counter) not in taken:
                    return name+str(counter)
                counter = counter + 1
            return None

    @staticmethod
    def _save_gather_error(message,job):
        '''
        Helper function to create an error during the gather stage.
        '''
        err = HarvestGatherError(message=message,job=job)
        err.save()
        log.error(message)

    @staticmethod
    def _save_object_error(message,obj,stage=u'Fetch'):
        '''
        Helper function to create an error during the fetch or import stage.
        '''
        err = HarvestObjectError(message=message,object=obj,stage=stage)
        err.save()
        log.error(message)

    def _create_harvest_objects(self, remote_ids, harvest_job):
        '''
        Given a list of remote ids and a Harvest Job, create as many Harvest Objects and
        return a list of its ids to be returned to the fetch stage.
        '''
        try:
            object_ids = []
            if len(remote_ids):
                for remote_id in remote_ids:
                    # Create a new HarvestObject for this identifier
                    obj = HarvestObject(guid = remote_id, job = harvest_job)
                    obj.save()
                    object_ids.append(obj.id)
                return object_ids
            else:
               self._save_gather_error('No remote datasets could be identified', harvest_job)
        except Exception, e:
            self._save_gather_error('%r' % e.message, harvest_job)

    # TODO: Move this to the model
    def _get_object_extra(self, harvest_object, key):
        '''
        Helper function for retrieving the value from a harvest object extra,
        given the key
        '''
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return None

    @maintain.deprecated('HarvesterBase._create_or_update_package() is '
            'deprecated and will be removed in a future version of '
            'ckanext-harvest. Instead, a harvester should inherit '
            'HarvesterBase.import_stage and override get_package_dict')
    def _create_or_update_package(self, package_dict, harvest_object):
        '''
        Creates a new package or updates an exisiting one according to the
        package dictionary provided. The package dictionary should look like
        the REST API response for a package:

        http://ckan.net/api/rest/package/statistics-catalunya

        Note that the package_dict must contain an id, which will be used to
        check if the package needs to be created or updated (use the remote
        dataset id).

        If the remote server provides the modification date of the remote
        package, add it to package_dict['metadata_modified'].

        '''
        try:
            # Change default schema
            schema = default_create_package_schema()
            schema['id'] = [ignore_missing, unicode]
            schema['__junk'] = [ignore]

            # Check API version
            if self.config:
                api_version = self.config.get('api_version','2')
                #TODO: use site user when available
                user_name = self.config.get('user',u'harvest')
            else:
                api_version = '2'
                user_name = u'harvest'

            context = {
                'model': model,
                'session': Session,
                'user': user_name,
                'api_version': api_version,
                'schema': schema,
            }

            tags = package_dict.get('tags', [])
            tags = [munge_tag(t) for t in tags if munge_tag(t) != '']
            tags = list(set(tags))
            package_dict['tags'] = tags

            # Check if package exists
            data_dict = {}
            data_dict['id'] = package_dict['id']
            try:
                existing_package_dict = get_action('package_show')(context, data_dict)

                # In case name has been modified when first importing. See issue #101.
                package_dict['name'] = existing_package_dict['name']

                # Check modified date
                if not 'metadata_modified' in package_dict or \
                   package_dict['metadata_modified'] > existing_package_dict.get('metadata_modified'):
                    log.info('Package with GUID %s exists and needs to be updated' % harvest_object.guid)
                    # Update package
                    context.update({'id':package_dict['id']})
                    new_package = get_action('package_update_rest')(context, package_dict)

                else:
                    log.info('Package with GUID %s not updated, skipping...' % harvest_object.guid)
                    return

            except NotFound:
                # Package needs to be created

                # Get rid of auth audit on the context otherwise we'll get an
                # exception
                context.pop('__auth_audit', None)

                # Check if name has not already been used
                package_dict['name'] = self._check_name(package_dict['name'])

                log.info('Package with GUID %s does not exist, let\'s create it' % harvest_object.guid)
                new_package = get_action('package_create_rest')(context, package_dict)
                harvest_object.package_id = new_package['id']

            # Flag the other objects linking to this package as not current anymore
            from ckanext.harvest.model import harvest_object_table
            conn = Session.connection()
            u = update(harvest_object_table) \
                    .where(harvest_object_table.c.package_id==bindparam('b_package_id')) \
                    .values(current=False)
            conn.execute(u, b_package_id=new_package['id'])
            Session.commit()

            # Flag this as the current harvest object

            harvest_object.package_id = new_package['id']
            harvest_object.current = True
            harvest_object.save()

            return True

        except ValidationError,e:
            log.exception(e)
            self._save_object_error('Invalid package with GUID %s: %r'%(harvest_object.guid,e.error_dict),harvest_object,'Import')
        except Exception, e:
            log.exception(e)
            self._save_object_error('%r'%e,harvest_object,'Import')

        return None

    def import_stage(self, harvest_object):
        '''The import_stage contains lots of boiler plate, updating the
        harvest_objects correctly etc, so inherit this method and customize the
        get_package_dict method.

        * HOExtra.status should have been set to 'new', 'changed' or 'deleted'
          in the gather or fetch stages.
        * It follows that checking that the metadata date has changed should
          have been done in the gather or fetch stages
        * harvest_object.source.config can control default additions to the
          package, for extras etc
        '''

        log = logging.getLogger(__name__ + '.import')
        log.debug('Import stage for harvest object: %s', harvest_object.id)

        if not harvest_object:
            # something has gone wrong with the code
            log.error('No harvest object received')
            self._save_object_error('System error')
            return False
        if harvest_object.content is None:
            # fetched object is blank - error with the harvested server
            self._save_object_error('Empty content for object %s' %
                                    harvest_object.id,
                                    harvest_object, 'Import')
            return False

        source_config = json.loads(harvest_object.source.config or '{}')

        status = self._get_object_extra(harvest_object, 'status')

        # Get the last harvested object (if any)
        previous_object = Session.query(HarvestObject) \
                          .filter(HarvestObject.guid==harvest_object.guid) \
                          .filter(HarvestObject.current==True) \
                          .first()

        if previous_object:
            previous_object.current = False
            harvest_object.package_id = previous_object.package_id
            previous_object.add()

        user = source_config.get('user', 'harvest')

        context = {'model': model, 'session': Session, 'user': user,
                   'api_version': 3, 'extras_as_string': True}

        if status == 'delete':
            # Delete package
            get_action('package_delete')(context, {'id': harvest_object.package_id})
            log.info('Deleted package {0} with guid {1}'.format(harvest_object.package_id, harvest_object.guid))
            previous_object.save()
            return True

        # Set defaults for the package_dict, mainly from the source_config
        package_dict_defaults = {}
        package_dict_defaults['id'] = harvest_object.package_id or unicode(uuid.uuid4())
        existing_dataset = model.Package.get(harvest_object.source.id)
        if existing_dataset:
            package_dict_defaults['name'] = existing_dataset.name
        if existing_dataset.owner_org:
            package_dict_defaults['owner_org'] = existing_dataset.owner_org
        else:
            package_dict_defaults['owner_org'] = harvest_object.source.publisher_id
        package_dict_defaults['tags'] = source_config.get('default_tags', [])
        package_dict_defaults['groups'] = source_config.get('default_groups', [])
        default_extras = source_config.get('default_extras', {})
        if default_extras:
            env = dict(harvest_source_id=harvest_object.job.source.id,
                       harvest_source_url=harvest_object.job.source.url.strip('/'),
                       harvest_source_title=harvest_object.job.source.title,
                       harvest_job_id=harvest_object.job.id,
                       harvest_object_id=harvest_object.id,
                       dataset_id=package_dict_defaults['id'])
            package_dict_defaults['extras'] = {}
            for key, value in default_extras.iteritems():
                # Look for replacement strings
                if isinstance(value, basestring):
                    value = value.format(env)
                package_dict_defaults['extras'][key] = value
        def merge(self, package_dict):
            '''Returns a dict based on the passed-in package_dict and adding
            default values from self (package_dict_defaults). Where the key is
            a string, the default is a fall-back for a blank value in the
            package_dict. Where the key is a list or dict, the values are
            merged.'''
            merged = package_dict.copy()
            for key in self:
                if isinstance(self[key], list):
                    merged[key] = self[key] + merged.get(key, [])
                    merged[key] = remove_duplicates_in_a_list(merged[key])
                elif isinstance(self[key], dict):
                    merged[key] = dict(self[key].items() +
                                       merged.get(key, {}).items())
                elif isinstance(self[key], basestring):
                    merged[key] = merged.get(key) or self[key]
                else:
                    raise NotImplementedError()
        package_dict_defaults.merge = merge

        try:
            package_dict = self.get_package_dict(harvest_object,
                                                 package_dict_defaults)
        except Exception, e:
            log.error('Harvest error in get_package_dict %r', harvest_object)
            self._save_object_error('System error', harvest_object, 'Import')
        if not package_dict:
            return False

        if source_config.get('clean_tags'):
            munge_tags(package_dict)

        # Create or update the package object

        if status == 'new':
            package_schema = logic.schema.default_create_package_schema()
        else:
            package_schema = logic.schema.default_update_package_schema()

        # Drop the validation restrictions on tags
        # (TODO: make this optional? get schema from config?)
        tag_schema = logic.schema.default_tags_schema()
        tag_schema['name'] = [not_empty, unicode]
        package_schema['tags'] = tag_schema
        context['schema'] = package_schema

        harvest_object.current = True

        if status == 'new':
            # We need to explicitly provide a package ID, otherwise
            # ckanext-spatial won't be be able to link the extent to the
            # package.
            if not package_dict.get('id'):
                package_dict['id'] = unicode(uuid.uuid4())
            package_schema['id'] = [unicode]

            # Save reference to the package on the object
            harvest_object.package_id = package_dict['id']
            # Defer constraints and flush so the dataset can be indexed with
            # the harvest object id (on the after_show hook from the harvester
            # plugin)
            model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
            model.Session.flush()

            if source_config.get('private_datasets', True):
                package_dict['private'] = True

            try:
                package_id = get_action('package_create')(context, package_dict)
                log.info('Created new package %s with guid %s', package_id, harvest_object.guid)
            except ValidationError, e:
                self._save_object_error('Validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
                return False
        elif status == 'changed':
            if previous_object:
                previous_object.current = False
            package_schema = logic.schema.default_update_package_schema()
            package_dict['id'] = harvest_object.package_id
            try:
                package_id = get_action('package_update')(context, package_dict)
                log.info('Updated package %s with guid %s', package_id, harvest_object.guid)
            except ValidationError, e:
                self._save_object_error('Validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
                return False

        model.Session.commit()
        return True

    def get_package_dict(self, harvest_object, package_dict_defaults,
                         source_config):
        '''
        Constructs a package_dict suitable to be passed to package_create or
        package_update. See documentation on
        ckan.logic.action.create.package_create for more details

        * name - a new package must have a unique name; if it had a name in the
          previous harvest, that will be in the package_dict_defaults.
        * resource.id - should be the same as the old object if updating a
          package
        * errors - call self._save_object_error() and return False
        * default values for name, owner_org, tags etc can be merged in using:
            package_dict = package_dict_defaults.merge(package_dict_harvested)

        If a dict is not returned by this function, the import stage will be
        cancelled.
        :param harvest_object: HarvestObject domain object (with access to
                               job and source objects)
        :type harvest_object: HarvestObject
        :param package_dict_defaults: Suggested/default values for the
          package_dict, based on the config, a previously harvested object, etc
        :type package_dict_defaults: dict
        :param source_config: The config of the harvest source
        :type source_config: dict

        :returns: A dataset dictionary (package_dict)
        :rtype: dict
        '''
        pass
