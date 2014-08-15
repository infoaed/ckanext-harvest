import datetime
from sqlalchemy import distinct, func

from ckan.model import Package,Group
from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject, \
                                  HarvestGatherError, HarvestObjectError


def harvest_source_dictize(source, context):
    out = source.as_dict()

    out['publisher_title'] = u''

    publisher_id = out.get('publisher_id')
    if publisher_id:
        group  = Group.get(publisher_id)
        if group:
            out['publisher_title'] = group.title

    return out

def harvest_job_dictize(job, context):
    model = context['model']

    out = job.as_dict()
    out['source'] = job.source_id
    out['objects'] = []
    out['gather_errors'] = []

    for obj in job.objects:
        out['objects'].append(obj.as_dict())

    for error in job.gather_errors:
        out['gather_errors'].append(error.as_dict())

    if context.get('return_stats', True):
        stats = model.Session.query(
            HarvestObject.report_status,
            func.count(HarvestObject.id).label('total_objects'))\
                .filter_by(harvest_job_id=job.id)\
                .group_by(HarvestObject.report_status).all()
        out['stats'] = {}
        for status, count in stats:
            out['stats'][status] = count

        # We actually want to check which objects had errors, because they
        # could have been added/updated anyway (eg bbox errors)
        count = model.Session.query(func.distinct(HarvestObjectError.harvest_object_id)) \
                          .join(HarvestObject) \
                          .filter(HarvestObject.harvest_job_id==job.id) \
                          .count()
        if count > 0:
          out['stats']['errored'] = count

        # Add gather errors to the error count
        count = model.Session.query(HarvestGatherError) \
                          .filter(HarvestGatherError.harvest_job_id==job.id) \
                          .count()
        if count > 0:
          out['stats']['errored'] = out['stats'].get('errored', 0) + count

    # DGU
    if context.get('return_object_errors', False):
        object_errors = model.Session.query(HarvestObjectError) \
                             .join(HarvestObject) \
                             .filter(HarvestObject.job==job)
        out['object_errors'] = object_errors.all()

    if context.get('return_error_summary', True):
        q = model.Session.query(HarvestObjectError.message, \
                                func.count(HarvestObjectError.message).label('error_count')) \
                          .join(HarvestObject) \
                          .filter(HarvestObject.harvest_job_id==job.id) \
                          .group_by(HarvestObjectError.message) \
                          .order_by('error_count desc') \
                          .limit(context.get('error_summmary_limit', 20))
        out['object_error_summary'] = q.all()
        q = model.Session.query(HarvestGatherError.message, \
                                func.count(HarvestGatherError.message).label('error_count')) \
                          .filter(HarvestGatherError.harvest_job_id==job.id) \
                          .group_by(HarvestGatherError.message) \
                          .order_by('error_count desc') \
                          .limit(context.get('error_summmary_limit', 20))
        out['gather_error_summary'] = q.all()
    return out

def harvest_object_dictize(obj, context):
    out = obj.as_dict()
    out['source'] = obj.harvest_source_id
    out['job'] = obj.harvest_job_id

    if obj.package:
        out['package'] = obj.package.id

    out['errors'] = []

    for error in obj.errors:
        out['errors'].append(error.as_dict())

    out['extras'] = {}
    for extra in obj.extras:
        out['extras'][extra.key] = extra.value

    return out

def _get_source_status(source, context):
    # REPLACED BY get.harvest_source_show_status
    '''
    Returns the harvest source's current job status and list of packages.

    detailed: calculate the details of the last_harvest too
    '''
    model = context.get('model')
    detailed = context.get('detailed',True)

    out = dict()

    job_count = HarvestJob.filter(source=source).count()

    out = {
           'job_count': 0,
           'next_harvest':'',
           'last_harvest_request':'',
           'last_harvest_statistics':{'added':0,'updated':0,'errors':0,'deleted':0},
           'last_harvest_errors':{'gather':[],'object':[]},
           'overall_statistics':{'added':0, 'errors':0},
           'packages':[]}

    if not job_count:
        out['msg'] = 'No jobs yet'
        return out
    else:
        out['job_count'] = job_count

    # Running job
    running_job = HarvestJob.filter(source=source,status=u'Running') \
                            .order_by(HarvestJob.created.desc()).first()
    if running_job:
        if running_job.gather_started:
            run_time = datetime.datetime.now() - running_job.gather_started
            minutes, seconds = divmod(run_time.seconds, 60)
            out['running_job'] = '%sm %ss' % (minutes, seconds)
        else:
            out['running_job'] = '(about to start)'
        if not running_job.gather_finished:
            out['running_job'] += ' - gathering records (%s so far)' % \
                                  len(running_job.objects)
        else:
            out['running_job'] += ' - fetch and import of %s records' % \
                                  len(running_job.objects)
    else:
        out['running_job'] = None

    # Get next scheduled job
    next_job = HarvestJob.filter(source=source,status=u'New').first()
    if next_job:
        out['next_harvest'] = 'Scheduled to start within 10 minutes'
    else:
        out['next_harvest'] = 'Not yet scheduled'

    # Get the last finished job
    last_job = HarvestJob.filter(source=source,status=u'Finished') \
               .order_by(HarvestJob.created.desc()).first()

    if last_job:
        out['last_harvest_request'] = str(last_job.gather_finished)

        #Get HarvestObjects from last job with links to packages
        if detailed:
            context_ = context.copy()
            context_.update(return_stats=True)
            harvest_job_dict = harvest_job_dictize(last_job, context_)
            statistics = out['last_harvest_statistics']
            statistics['added'] = harvest_job_dict['stats'].get('new',0)
            statistics['updated'] = harvest_job_dict['stats'].get('updated',0)
            statistics['deleted'] = harvest_job_dict['stats'].get('deleted',0)
            statistics['errors'] = (harvest_job_dict['stats'].get('errored',0) +
                                    len(last_job.gather_errors))

        if detailed:
            # Last harvest errors
            # We have the gathering errors in last_job.gather_errors, so let's also
            # get also the object errors.
            object_errors = model.Session.query(HarvestObjectError).join(HarvestObject) \
                                .filter(HarvestObject.job==last_job)

            for gather_error in last_job.gather_errors:
                out['last_harvest_errors']['gather'].append(gather_error.message)

            for object_error in object_errors:
                err = {'object_id':object_error.object.id,'object_guid':object_error.object.guid,'message': object_error.message}
                out['last_harvest_errors']['object'].append(err)

        # Overall statistics
        packages = model.Session.query(distinct(HarvestObject.package_id),Package.name) \
                .join(Package).join(HarvestSource) \
                .filter(HarvestObject.source==source) \
                .filter(HarvestObject.current==True) \
                .filter(Package.state==u'active')

        out['overall_statistics']['added'] = packages.count()
        if detailed:
            out['packages'] = [package.name for package in packages]

        gather_errors = model.Session.query(HarvestGatherError) \
                .join(HarvestJob).join(HarvestSource) \
                .filter(HarvestJob.source==source).count()

        object_errors = model.Session.query(HarvestObjectError) \
                .join(HarvestObject).join(HarvestJob).join(HarvestSource) \
                .filter(HarvestJob.source==source).count()
        out['overall_statistics']['errors'] = gather_errors + object_errors
    else:
        out['last_harvest_request'] = 'Not yet harvested'

    return out

