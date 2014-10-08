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
from ckan.lib.munge import munge_title_to_name, munge_tag

from ckanext.harvest.model import HarvestObject, HarvestGatherError, \
                                    HarvestObjectError

from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckan.lib.helpers import json

log = logging.getLogger(__name__)


def munge_tags(package_dict):
    tags = package_dict.get('tags', [])
    tags = [munge_tag(t['name']) for t in tags if t]
    tags = [t for t in tags if t != '__']  # i.e. just padding
    tags = remove_duplicates_in_a_list(tags)
    package_dict['tags'] = [dict(name=name) for name in tags]


def remove_duplicates_in_a_list(list_):
    seen = set()
    seen_add = seen.add
    return [x for x in list_ if not (x in seen or seen_add(x))]


class HarvesterBase(SingletonPlugin):
    '''
    Generic class for harvesters, providing them with a common import_stage and
    various helper functions. A harvester doesn't have to derive from this - it
    should just have:

        implements(IHarvester)

    however this base class avoids lots of work dealing with HarvestObjects.
    '''
    config = None

    munge_tags = munge_tags

    @staticmethod
    def munge_title_to_name(title):
        '''
        Creates a URL friendly name from a title. Compared to the ckan method
        munge_title_to_name, for spaces this version uses dashes rather than
        underscores.
        '''
        name = munge_title_to_name(title).replace('_', '-')
        while '--' in name:
            name = name.replace('--', '-')
        return name
    _gen_new_name = munge_title_to_name

    @staticmethod
    def check_name(name):
        '''
        Checks if a package name already exists in the database, and adds
        a counter at the end if it does exist.
        '''
        like_q = u'%s%%' % name
        pkg_query = Session.query(Package).filter(Package.name.ilike(like_q)).limit(1000)
        taken = [pkg.name for pkg in pkg_query]
        if name not in taken:
            return name
        else:
            counter = 1
            while counter < 1001:
                if name+str(counter) not in taken:
                    return name+str(counter)
                counter = counter + 1
            return None
    _check_name = check_name  # for backwards compatibility

    @staticmethod
    def extras_from_dict(extras_dict):
        '''Takes extras in the form of a dict and returns it in the form of a
        list of dicts.  A single extras dict is convenient to fill with values
        in get_package_dict() but it needs to return the extras as a list of
        dicts, to suit package_update.

        e.g.
        >>> HarvesterBase.extras_from_dict({'theme': 'environment', 'freq': 'daily'})
        [{'key': 'theme', 'value': 'environment'}, {'key': 'freq', 'value': 'daily'}]
        '''
        return [{'key': key, 'value': value} for key, value in extras_dict.items()]

    save_gather_error = HarvestGatherError.create
    _save_gather_error = HarvestGatherError.create  # for backwards compatibility
    save_object_error = HarvestObjectError.create
    _save_object_error = HarvestObjectError.create  # for backwards compatibility

    def create_harvest_objects(self, remote_ids, harvest_job):
        '''
        Given a list of remote ids and a Harvest Job, create as many Harvest Objects and
        return a list of their ids to be passed to the fetch stage.

        TODO: Not sure it is worth keeping this function
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
    _create_harvest_objects = create_harvest_objects  # for backwards compat

    @maintain.deprecated('Use the harvest_object.get_extra method instead.')
    def _get_object_extra(self, harvest_object, key):
        '''
        Deprecated!

        Helper function for retrieving the value from a harvest object extra,
        given the key
        '''
        return harvest_object.get_extra(key)

    @maintain.deprecated('HarvesterBase._create_or_update_package() is '
            'deprecated and will be removed in a future version of '
            'ckanext-harvest. Instead, a harvester should inherit '
            'HarvesterBase.import_stage and override get_package_dict')
    def _create_or_update_package(self, package_dict, harvest_object):
        '''
        DEPRECATED!

        Creates a new package or updates an exisiting one according to the
        package dictionary provided. The package dictionary should look like
        the REST API response for a package:

        http://ckan.net/api/rest/package/statistics-catalunya

        Note that the package_dict must contain an id, which will be used to
        check if the package needs to be created or updated (use the remote
        dataset id).

        If the remote server provides the modification date of the remote
        package, add it to package_dict['metadata_modified'].

        TODO: Not sure it is worth keeping this function. If useful it should
        use the output of package_show logic function (maybe keeping support
        for rest api based dicts
        '''
        try:
            # Change default schema
            schema = default_create_package_schema()
            schema['id'] = [ignore_missing, unicode]
            schema['__junk'] = [ignore]

            # Check API version
            if self.config:
                try:
                    api_version = int(self.config.get('api_version', 2))
                except ValueError:
                    raise ValueError('api_version must be an integer')
                #TODO: use site user when available
                user_name = self.config.get('user',u'harvest')
            else:
                api_version = 2
                user_name = u'harvest'

            context = {
                'model': model,
                'session': Session,
                'user': user_name,
                'api_version': api_version,
                'schema': schema,
                'ignore_auth': True,
            }

            if self.config and self.config.get('clean_tags', False):
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
                    package_dict.setdefault('name',
                            existing_package_dict['name'])
                    new_package = get_action('package_update_rest')(context, package_dict)

                else:
                    log.info('Package with GUID %s not updated, skipping...' % harvest_object.guid)
                    return

                # Flag the other objects linking to this package as not current anymore
                from ckanext.harvest.model import harvest_object_table
                conn = Session.connection()
                u = update(harvest_object_table) \
                        .where(harvest_object_table.c.package_id==bindparam('b_package_id')) \
                        .values(current=False)
                conn.execute(u, b_package_id=new_package['id'])

                # Flag this as the current harvest object

                harvest_object.package_id = new_package['id']
                harvest_object.current = True
                harvest_object.save()

            except NotFound:
                # Package needs to be created

                # Get rid of auth audit on the context otherwise we'll get an
                # exception
                context.pop('__auth_audit', None)

                # Set name if not already there
                package_dict.setdefault('name', self.munge_title_to_name(package_dict['title']))

                log.info('Package with GUID %s does not exist, let\'s create it' % harvest_object.guid)
                harvest_object.current = True
                harvest_object.package_id = package_dict['id']
                # Defer constraints and flush so the dataset can be indexed with
                # the harvest object id (on the after_show hook from the harvester
                # plugin)
                harvest_object.add()

                model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
                model.Session.flush()

                new_package = get_action('package_create_rest')(context, package_dict)

            Session.commit()
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

        status = harvest_object.get_extra('status')
        if not status in ['new', 'changed', 'deleted']:
            log.error('Status is not set correctly: %r', status)
            self._save_object_error('System error')
            return False

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
        package_dict_defaults = PackageDictDefaults()
        package_dict_defaults['id'] = harvest_object.package_id or unicode(uuid.uuid4())
        existing_dataset = model.Package.get(harvest_object.package_id)
        if existing_dataset:
            package_dict_defaults['name'] = existing_dataset.name
        if source_config.get('remote_orgs') not in ('only_local', 'create'):
            # Assign owner_org to the harvest_source's publisher
            #master would get the harvest_object.source.publisher_id this way:
            #source_dataset = get_action('package_show')(context, {'id': harvest_object.source.id})
            #local_org = source_dataset.get('owner_org')
            package_dict_defaults['owner_org'] = harvest_object.source.publisher_id
        elif existing_dataset and existing_dataset.owner_org:
            package_dict_defaults['owner_org'] = existing_dataset.owner_org
        package_dict_defaults['tags'] = source_config.get('default_tags', [])
        package_dict_defaults['groups'] = source_config.get('default_groups', [])
        package_dict_defaults['extras'] = {
            'import_source': 'harvest',  # to identify all harvested datasets
            'harvest_object_id': harvest_object.id,
            'guid': harvest_object.guid,
            'metadata-date': harvest_object.metadata_modified_date.strftime('%Y-%m-%d'),
            }
        default_extras = source_config.get('default_extras', {})
        if default_extras:
            env = dict(harvest_source_id=harvest_object.job.source.id,
                       harvest_source_url=harvest_object.job.source.url.strip('/'),
                       harvest_source_title=harvest_object.job.source.title,
                       harvest_job_id=harvest_object.job.id,
                       harvest_object_id=harvest_object.id,
                       dataset_id=package_dict_defaults['id'])
            for key, value in default_extras.iteritems():
                # Look for replacement strings
                if isinstance(value, basestring):
                    value = value.format(env)
                package_dict_defaults['extras'][key] = value

        try:
            package_dict = self.get_package_dict(harvest_object,
                                                 package_dict_defaults,
                                                 source_config,
                                                 existing_dataset)
        except Exception, e:
            log.exception('Harvest error in get_package_dict %r', harvest_object)
            self._save_object_error('System error', harvest_object, 'Import')
            return False
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
            if model.engine_is_pg():
                model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
                model.Session.flush()

            if source_config.get('private_datasets', True):
                package_dict['private'] = True

            log.debug('package_create: %r', package_dict)
            try:
                package_dict_created = get_action('package_create')(context, package_dict)
                log.info('Created new package name=%s id=%s guid=%s', package_dict.get('name'), package_dict_created['id'], harvest_object.guid)
            except ValidationError, e:
                self._save_object_error('Validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
                return False
        elif status == 'changed':
            if previous_object:
                previous_object.current = False
            package_schema = logic.schema.default_update_package_schema()
            package_dict['id'] = harvest_object.package_id
            log.debug('package_update: %r', package_dict)
            try:
                package_dict_updated = get_action('package_update')(context, package_dict)
                log.info('Updated package name=%s id=%s guid=%s', package_dict.get('name'), package_dict_created['id'], harvest_object.guid)
            except ValidationError, e:
                self._save_object_error('Validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
                return False

        model.Session.commit()
        return True

    def get_package_dict(self, harvest_object, package_dict_defaults,
                         source_config, existing_dataset):
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
            (NB default extras should be a dict, not a list of dicts)
        * extras should be converted from a dict to a list of dicts before
          returning - use extras_from_dict()

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
        :param source_config: The dataset as it was harvested last time. Needed
          to set resource IDs the same as with existing resources.
        :type existing_dataset: Package

        :returns: A dataset dictionary (package_dict)
        :rtype: dict
        '''
        pass

class PackageDictDefaults(dict):
    def merge(self, package_dict):
        '''
        Returns a dict based on the passed-in package_dict and adding default
        values from self. Where the key is a string, the default is a
        fall-back for a blank value in the package_dict. Where the key is a
        list or dict, the values are merged.
        '''
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
        return merged
