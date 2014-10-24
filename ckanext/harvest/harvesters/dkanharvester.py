from ckanharvester import CKANHarvester
import json
from ckan import model
import ckan.lib.munge as munge

MIMETYPE_FORMATS = {
    'text/html': 'HTML',
    'text/csv': 'CSV',
    'text/xml': 'XML',
    'application/pdf': 'PDF',
    'application/zip': 'ZIP',
    'application/rdf+xml': 'RDF',
    'application/json': 'JSON',
    'application/vnd.ms-excel': 'XLS',
    'application/vnd.google-earth.kml+xml': 'KML',
    'application/msword': 'DOC',
}

class DKANHarvester(CKANHarvester):
    def info(self):
        return {
            'name': 'dkan',
            'title': 'DKAN',
            'description': 'Harvests remote DKAN instances',
            'form_config_interface':'Text'
        }

    def _get_rest_api_offset(self):
        return '/api/3/action'

    def _get_all_packages(self, base_rest_url, harvest_job):
        # Request all remote packages
        url = base_rest_url + '/package_list'
        try:
            content = self._get_content(url)
        except Exception, e:
            self._save_gather_error('Unable to get content for URL: %s: %s'
                                    % (url, e), harvest_job)
            return None

        packages = json.loads(content)['result']
        if '2fcd2e80-2e46-4660-a20d-665d644d1ded' in packages:
            packages.remove('2fcd2e80-2e46-4660-a20d-665d644d1ded')

        return packages

    def _get_package(self, base_url, harvest_object):
        url = base_url + self._get_rest_api_offset() + '/package_show/' + harvest_object.guid

        # Get contents
        try:
            content = json.loads(self._get_content(url))
            package = content['result'][0]

            if 'extras' not in package:
                package['extras'] = {}

            if 'name' not in package:
                package['name'] = munge.munge_title_to_name(package['title'])

            if 'description' in package:
                package['notes'] = package['description']

            for license in model.Package.get_license_register().values():
                if license.title == package['license_title']:
                    package['license_id'] = license.id
                    break

            # DGU ONLY: Guess theme from other metadata
            try:
                from ckanext.dgu.lib.theme import categorize_package, PRIMARY_THEME, SECONDARY_THEMES
                themes = categorize_package(package)
                if themes:
                    package['extras'][PRIMARY_THEME] = themes[0]
                    package['extras'][SECONDARY_THEMES] = themes[1:]
            except ImportError:
                pass

            for resource in package['resources']:
                resource['description'] = resource['title']

                if resource['revision_id']:
                    del resource['revision_id']

                if 'format' not in resource:
                    resource['format'] = MIMETYPE_FORMATS.get(resource.get('mimetype'), '')

            return url, json.dumps(package)
        except Exception, e:
            self._save_object_error(
                'Unable to get content for package: %s: %r' % (url, e),
                harvest_object)
            return None, None

    def _fix_tags(self, package_dict):
        pass
