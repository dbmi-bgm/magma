import sys, os

from dcicutils import ff_utils
from dcicutils.s3_utils import s3Utils
from tibanna.job import Job
from functools import cached_property
from magma.magma_constants import *
from magma_ff.utils import JsonObject
from typing import Optional
from requests.exceptions import HTTPError


class FFWfrUtils(object):
    def __init__(self, env):
        """
        :param env: e.g. 'fourfront-cgap', 'fourfront-cgap-wolf'
        :type env: str
        """
        self.env = env

        # Cache for metadata
        self._metadata = dict()
        # Cache for access key
        self._ff_key = None

    def wfr_run_uuid(self, job_id):
        """This is the function to be used by Magma.
        """
        wfr_meta = self.wfr_metadata(job_id)
        if not wfr_meta:
            return None
        return wfr_meta['uuid']

    def wfr_run_status(self, job_id):
        """This is the function to be used by Magma.
        Return the status of the run associated with specified job_id.
        If run associated with job_id is not found, we consider it failed.
        """
        wfr_meta = self.wfr_metadata(job_id)
        if not wfr_meta:
            return 'error'
        else:
            return wfr_meta['run_status']

    def get_minimal_processed_output(self, job_id):
        """This is the function to be used by Magma.
        Return a list of {'argument_name': <arg_name>, 'file': <uuid>}
        for all processed file output. If no output, return None.
        """
        wfr_output = self.wfr_output(job_id)
        return self.filter_wfr_output_minimal_processed(wfr_output)

    def wfr_output(self, job_id):
        """Return the raw output from the run metadata.
        Return None if run metadata associated with the job is not found or
        does not have associated output files.
        """
        if self.wfr_metadata(job_id):
            return self.wfr_metadata(job_id).get('output_files', None)
        else:
            return None

    def wfr_metadata(self, job_id):
        """Get portal run metadata from job_id.
        Return None if a run associated with job id cannot be found.
        """
        # Use cache
        if job_id in self._metadata:
            return self._metadata[job_id]
        # Search by job id
        query='/search/?type=WorkflowRun&awsem_job_id=%s' % job_id
        try:
            search_res = ff_utils.search_metadata(query, key=self.ff_key)
        except Exception as e:
            raise FdnConnectionException(e)
        if search_res:
            self._metadata[job_id] = search_res[0]
            return self._metadata[job_id]
        else:
            # find it from dynamoDB
            job_info = Job.info(job_id)
            if not job_info:
                return None
            wfr_uuid = job_info.get('WorkflowRun uuid', '')
            if not wfr_uuid:
                return None
            self._metadata[job_id] = ff_utils.get_metadata(wfr_uuid, key=self.ff_key)
            return self._metadata[job_id]

    @property
    def ff_key(self):
        """Get access key for the portal.
        """
        # Use cache
        if not self._ff_key:
            # Use tibanna key for now
            self._ff_key = s3Utils(env=self.env).get_access_keys('access_key_tibanna')
        return self._ff_key

    @staticmethod
    def filter_wfr_output_minimal_processed(wfr_output):
        """Return a list of {'argument_name': <arg_name>, 'file': <uuid>}
        for all processed file output.
        """
        if wfr_output:
            return [{'argument_name': opf['workflow_argument_name'],
                     'file': opf['value']['uuid']} \
                        for opf in wfr_output \
                            if opf['type'] == 'Output processed file']
        else:
            return None

#end class

class FdnConnectionException(Exception):
    pass

#end class


class FFMetaWfrUtils(object):
    """Class for accessing status and cost metadata of a MetaWorkflow Run from CGAP portal."""

    def __init__(self, auth_key: JsonObject) -> None:
        """ 
        Initialize FFMetaWfrUtils object, setting basic attributes.
        
        :param auth_key: Authorization keys for C4 account
        """
        self._auth_key = auth_key

    def get_meta_workflow_run_status(self, meta_workflow_run_identifier: str) -> str:
        """
        Return the status of the MetaWorkflow Run associated with specified ID.

        :param meta_workflow_run_identifier: Identifier (e.g. UUID, @id) for 
            MetaWorkflow Run to be searched
        :return: the status of the specified MetaWorkflow Run
        """
        meta_workflow_run_portal_output = self._retrieve_meta_workflow_run(meta_workflow_run_identifier)
        
        # TODO: for now, just assuming it will have this attribute
        # check this in integrated testing
        return meta_workflow_run_portal_output[FINAL_STATUS]

    def get_meta_workflow_run_cost(self, meta_workflow_run_identifier: str) -> float:
        """
        Return the cost of the MetaWorkflow Run associated with specified ID.
        If no cost attribute found, return cost as 0.

        :param meta_workflow_run_identifier: Identifier (e.g. UUID, @id) for
            MetaWorkflow Run to be searched
        :return: the cost of the specified MetaWorkflow Run
        """
        meta_workflow_run_portal_output = self._retrieve_meta_workflow_run(meta_workflow_run_identifier)

        if COST in meta_workflow_run_portal_output:
            return meta_workflow_run_portal_output[COST]

        return float(0)

    def _retrieve_meta_workflow_run(self, meta_workflow_run_identifier: str) -> JsonObject:
        """
        Get portal MetaWorkflow Run metadata JSON using its identifier.
        Raises Exception if GET request is unsuccessful.

        :param meta_workflow_run_identifier: Identifier (e.g. UUID, @id) for
            MetaWorkflow Run to be searched
        :return: Portal JSON object representing this MetaWorkflow Run and its metadata
        """
        # Use cache if ID is an existent key
        if meta_workflow_run_identifier in self._meta_workflow_runs_cache:
            return self._meta_workflow_runs_cache[meta_workflow_run_identifier]

        # Otherwise retrieve this metadata from the portal
        try:
            result = ff_utils.get_metadata(
                meta_workflow_run_identifier, key=self._auth_key
            )
        except Exception as err:
            raise HTTPError(err, f"GET request unsuccessful for MetaWorkflow Run using the following ID:\
                {meta_workflow_run_identifier}") from err

        # Add GET request result to cache
        self._meta_workflow_runs_cache[meta_workflow_run_identifier] = result
        return result

    @cached_property
    def _meta_workflow_runs_cache(self) -> dict:
        """
        Cache for MetaWorkflowRun metadata retrieved from CGAP portal.
        Can save several MetaWorkflow Run metadata dicts at a time.
        Initially empty, modified as MetaWorkflow Runs are retrieved.
        Key-value = uuid-metadata_dict
        """
        return {}
