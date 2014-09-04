
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
            package['tags'] = [tag['name'] for tag in package['tags']] # Kludge
            if 'extras' not in package:
                package['extras'] = {}
            return url, json.dumps(package)
        except Exception, e:
            self._save_object_error(
                'Unable to get content for package: %s: %r' % (url, e),
                harvest_object)
            return None, None

    def _fix_tags(self, package_dict):
        pass
