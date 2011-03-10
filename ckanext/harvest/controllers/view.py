from ckan.lib.helpers import json
import urllib2
import ckan.lib.helpers as h
from ckan.lib.base import BaseController, c, g, request, \
                          response, session, render, config, abort, redirect
#from ..dictization import *

class ViewController(BaseController):

    api_url = config.get('ckan.api_url', 'http://localhost:5000').rstrip('/')+'/api/2/rest'
    api_key = config.get('ckan.harvesting.api_key')

    def _do_request(self,url,data = None):

        http_request = urllib2.Request(
            url = url,
            headers = {'Authorization' : self.api_key}
        )

        if data:
            http_request.add_data(data)
        
        try:
            return urllib2.urlopen(http_request)
        except urllib2.HTTPError as e:
            raise Exception('The API call returned an error: ' + str(e.getcode()) + \
                    ' ' + e.msg + ' [' + e.url + ']')



    def index(self):
        # Request all harvesting sources
        sources_url = self.api_url + '/harvestsource'
        
        doc = self._do_request(sources_url).read()
        sources_ids = json.loads(doc)

        source_url = sources_url + '/%s'
        sources = []
        
        # For each source, request its details
        for source_id in sources_ids:
            doc = self._do_request(source_url % source_id).read()
            sources.append(json.loads(doc))

        c.sources = sources
        return render('ckanext/harvest/index.html')
    
    def create(self):

        # This is the DGU form API, so we don't use self.api_url
        form_url = config.get('ckan.api_url', '/').rstrip('/') + \
                   '/api/2/form/harvestsource/create'
        if request.method == 'GET':
            # Request the fields
            c.form = self._do_request(form_url).read()

            return render('ckanext/harvest/create.html')
        if request.method == 'POST':
            # Build an object like the one expected by the DGU form API
            data = {
                'form_data':
                    {'HarvestSource--url': request.POST['HarvestSource--url'],
                     'HarvestSource--description': request.POST['HarvestSource--description']},
                'user_ref':'',
                'publisher_ref':''
            }
            data = json.dumps(data)

            try:
                r = self._do_request(form_url,data)
    
                h.flash_success('Harvesting source added successfully')
                redirect(h.url_for(controller='harvest', action='index'))
            except urllib2.HTTPError as e:
                raise Exception('The forms API returned an error:' + str(e.getcode()) + ' ' + e.msg )
                        

    def show(self,id):
        sources_url = self.api_url + '/harvestsource/%s' % id
        doc = self._do_request(sources_url).read()
        c.source = json.loads(doc)

        return render('ckanext/harvest/show.html')