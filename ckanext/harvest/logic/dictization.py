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
        group = Group.get(publisher_id)
        if group:
            out['publisher_title'] = group.title
            out['publisher_name'] = group.name
            out['is_organization'] = group.is_organization

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
        out['stats'] = job.get_object_report_statuses()

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

# _get_source_status REPLACED BY get.harvest_source_show_status
