# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
#	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Integration tests for the EMR Serverless Application resource.
"""

import pytest
import time
import logging
import uuid

from acktest.resources import random_suffix_name
from acktest.k8s import resource as k8s
from acktest.k8s import condition
from acktest import tags
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_emrserverless_resource
from e2e.replacement_values import REPLACEMENT_VALUES

RESOURCE_PLURAL = "applications"

CREATE_WAIT_AFTER_SECONDS = 30
MODIFY_WAIT_AFTER_SECONDS = 10
DELETE_WAIT_AFTER_SECONDS = 30


INITIAL_TAGS = {
    "environment": "test",
    "team": "data-platform",
}

def _create_application(resource_name: str, application_type: str = "SPARK"):
    """Helper to create an Application CR and return (ref, cr)."""
    replacements = REPLACEMENT_VALUES.copy()
    replacements["APPLICATION_NAME"] = resource_name
    replacements["RELEASE_LABEL"] = "emr-7.0.0"
    replacements["APPLICATION_TYPE"] = application_type
    
    resource_data = load_emrserverless_resource(
        "application",
        additional_replacements=replacements,
    )

    resource_data["spec"]["tags"] = INITIAL_TAGS
    
    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
        resource_name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)
    
    return (ref, cr)


@pytest.fixture(scope="module")
def simple_application(emrserverless_client):
    """Creates a simple Spark Application for testing."""
    resource_name = random_suffix_name("ack-test-app", 24)
    
    (ref, cr) = _create_application(resource_name)
    logging.debug(cr)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    yield (ref, cr)

    # Teardown
    try:
        _, deleted = k8s.delete_custom_resource(ref, 3, 10)
        assert deleted
    except:
        pass


@service_marker
@pytest.mark.canary
class TestApplication:
    def test_create_delete(self, emrserverless_client, simple_application):
        (ref, cr) = simple_application

        # Wait for the resource to be synced
        time.sleep(CREATE_WAIT_AFTER_SECONDS)
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)
        
        # Get the application ID from status
        cr = k8s.get_resource(ref)
        assert "status" in cr
        assert "id" in cr["status"]
        application_id = cr["status"]["id"]
        
        # Verify the resource exists in AWS
        response = emrserverless_client.get_application(
            applicationId=application_id
        )
        
        app = response["application"]
        
        # Verify basic properties
        assert app["applicationId"] == application_id
        assert app["name"] == cr["spec"]["name"]
        assert app["releaseLabel"] == cr["spec"]["releaseLabel"]
        
        # Verify state is one of the expected synced states
        assert app["state"] in ["CREATED", "STARTED", "STOPPED"]
        
        # Verify status fields are populated
        assert "ackResourceMetadata" in cr["status"]
        assert "arn" in cr["status"]["ackResourceMetadata"]


    def test_update_auto_stop_configuration(self, emrserverless_client, simple_application):
        (ref, cr) = simple_application
        
        # Wait for initial sync
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)
        
        # Get the application ID from status
        cr = k8s.get_resource(ref)
        application_id = cr["status"]["id"]
        
        # Update auto stop configuration
        updates = {
            "spec": {
                "autoStopConfiguration": {
                    "enabled": True,
                    "idleTimeoutMinutes": 30
                }
            }
        }
        
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)
        
        # Wait for the update to sync
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)
        
        # Verify the update in AWS
        response = emrserverless_client.get_application(
            applicationId=application_id
        )
        
        app = response["application"]
        assert app["autoStopConfiguration"]["enabled"] == True
        assert app["autoStopConfiguration"]["idleTimeoutMinutes"] == 30

    def test_update_maximum_capacity(self, emrserverless_client, simple_application):
        (ref, cr) = simple_application
        
        # Wait for initial sync
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)
        
        # Get the application ID from status
        cr = k8s.get_resource(ref)
        application_id = cr["status"]["id"]
        
        # Update maximum capacity
        updates = {
            "spec": {
                "maximumCapacity": {
                    "cpu": "4 vCPU",
                    "memory": "16 GB"
                }
            }
        }
        
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)
        
        # Wait for the update to sync
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)
        
        # Verify the update in AWS
        response = emrserverless_client.get_application(
            applicationId=application_id
        )
        
        app = response["application"]
        assert "maximumCapacity" in app
        assert app["maximumCapacity"]["cpu"] == "4 vCPU"
        assert app["maximumCapacity"]["memory"] == "16 GB"

    def test_crud_tags(self, emrserverless_client, simple_application):
        (ref, cr) = simple_application
        
        # Wait for initial sync
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)
        
        # Get the application ARN from status
        cr = k8s.get_resource(ref)
        application_arn = cr["status"]["ackResourceMetadata"]["arn"]
        
        # Test 1: Verify initial tags from creation
        response = emrserverless_client.list_tags_for_resource(resourceArn=application_arn)
        initial_tags = response["tags"]
        
        tags.assert_ack_system_tags(tags=initial_tags)
        tags.assert_equal_without_ack_tags(expected=INITIAL_TAGS, actual=initial_tags)
        
        # Test 2: Update tags via patch
        updated_tags = {"environment": "staging", "team": "data-platform", "new-tag" : "new-tag-value"}
        updates = {
            "spec": {
                "tags": updated_tags
            }
        }
        
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)
        
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)
        
        response = emrserverless_client.list_tags_for_resource(resourceArn=application_arn)
        latest_tags = response["tags"]
        
        tags.assert_ack_system_tags(tags=latest_tags)
        tags.assert_equal_without_ack_tags(expected=updated_tags, actual=latest_tags)
        
        # Test 3: Remove a tag key by using replace instead of patch.
        # Kubernetes strategic/JSON merge patch on a map only adds/updates
        # keys — it never removes missing keys. To delete "new-tag" we must
        # replace the full resource so the tags map is set exactly.
        updated_tags = {"environment": "production", "team": "data-platform"}
        cr = k8s.get_resource(ref)
        cr["spec"]["tags"] = updated_tags
        k8s.replace_custom_resource(ref, cr)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)
        
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)
        
        response = emrserverless_client.list_tags_for_resource(resourceArn=application_arn)
        latest_tags = response["tags"]
        
        tags.assert_ack_system_tags(tags=latest_tags)
        tags.assert_equal_without_ack_tags(expected=updated_tags, actual=latest_tags)
        
        # Test 4: Remove all user tags
        cr = k8s.get_resource(ref)
        cr["spec"]["tags"] = {}
        k8s.replace_custom_resource(ref, cr)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)
        
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)
        
        response = emrserverless_client.list_tags_for_resource(resourceArn=application_arn)
        latest_tags = response["tags"]
        
        tags.assert_ack_system_tags(tags=latest_tags)
        tags.assert_equal_without_ack_tags(expected={}, actual=latest_tags)

    def test_delete(self, emrserverless_client):
        """Test that deleting the K8s resource deletes the AWS Application
        and the K8s CR is removed after the AWS resource reaches TERMINATED."""
        resource_name = random_suffix_name("ack-test-app-del", 24)
        
        (ref, cr) = _create_application(resource_name)
        
        assert cr is not None
        time.sleep(CREATE_WAIT_AFTER_SECONDS)
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)
        
        # Get the application ID from status
        cr = k8s.get_resource(ref)
        application_id = cr["status"]["id"]
        
        # Verify the Application exists in AWS
        response = emrserverless_client.get_application(
            applicationId=application_id
        )
        assert response["application"] is not None
        
        # Delete the K8s resource
        _, deleted = k8s.delete_custom_resource(ref, 3, 10)
        assert deleted
        
        # Poll for AWS resource to reach TERMINATED state
        aws_terminated = False
        max_wait_periods = 30
        wait_period_length = 10
        
        for _ in range(max_wait_periods):
            time.sleep(wait_period_length)
            
            try:
                response = emrserverless_client.get_application(
                    applicationId=application_id
                )
                if response["application"]["state"] == "TERMINATED":
                    aws_terminated = True
                    break
            except emrserverless_client.exceptions.ResourceNotFoundException:
                aws_terminated = True
                break
        
        assert aws_terminated, (
            f"Application {application_id} was not deleted from AWS "
            f"after {max_wait_periods * wait_period_length} seconds"
        )
        
        # Verify the K8s CR is fully removed (finalizer cleaned up)
        time.sleep(DELETE_WAIT_AFTER_SECONDS)
        assert not k8s.get_resource_exists(ref), (
            f"K8s resource {resource_name} still exists after AWS application "
            f"reached TERMINATED state"
        )

    def test_recreate_after_terminated(self, emrserverless_client):
        """Test that when an AWS Application is terminated out-of-band,
        the controller treats it as not found and recreates it."""
        resource_name = random_suffix_name("ack-test-app-term", 24)
        
        (ref, cr) = _create_application(resource_name)
        
        assert cr is not None
        time.sleep(CREATE_WAIT_AFTER_SECONDS)
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)
        
        # Get the original application ID
        cr = k8s.get_resource(ref)
        original_application_id = cr["status"]["id"]
        
        # Terminate the AWS application out-of-band via boto3
        # First stop it (required before deleting)
        try:
            emrserverless_client.stop_application(
                applicationId=original_application_id
            )
        except Exception:
            pass  # May already be stopped
        
        # Wait for STOPPED state before deleting
        for _ in range(30):
            time.sleep(10)
            try:
                response = emrserverless_client.get_application(
                    applicationId=original_application_id
                )
                state = response["application"]["state"]
                if state in ["STOPPED", "CREATED"]:
                    break
            except emrserverless_client.exceptions.ResourceNotFoundException:
                break
        
        # Delete the AWS application out-of-band
        emrserverless_client.delete_application(
            applicationId=original_application_id
        )
        
        # Wait for the original application to reach TERMINATED
        for _ in range(30):
            time.sleep(10)
            try:
                response = emrserverless_client.get_application(
                    applicationId=original_application_id
                )
                if response["application"]["state"] == "TERMINATED":
                    break
            except emrserverless_client.exceptions.ResourceNotFoundException:
                break
        
        # Trigger a reconcile by patching a spec field. Annotation
        # changes don't trigger reconciliation in controller-runtime,
        # but spec changes do. Toggle idleTimeoutMinutes to force it.
        k8s.patch_custom_resource(ref, {
            "spec": {
                "autoStopConfiguration": {
                    "idleTimeoutMinutes": 20
                }
            }
        })

        # The controller should detect TERMINATED as NotFound and recreate.
        # Wait for the resource to get a new application ID and sync.
        recreated = False
        for _ in range(30):
            time.sleep(10)
            try:
                cr = k8s.get_resource(ref)
                if cr is None:
                    continue
                new_id = cr.get("status", {}).get("id")
                if new_id is not None and new_id != original_application_id:
                    recreated = True
                    break
            except Exception:
                continue
        
        assert recreated, (
            f"Controller did not recreate the application after the original "
            f"{original_application_id} was terminated out-of-band"
        )
        
        # Verify the new application is synced
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)
        
        # Verify the new application exists in AWS
        cr = k8s.get_resource(ref)
        new_application_id = cr["status"]["id"]
        response = emrserverless_client.get_application(
            applicationId=new_application_id
        )
        assert response["application"]["state"] in ["CREATED", "STARTED", "STOPPED"]
        
        # Cleanup
        _, deleted = k8s.delete_custom_resource(ref, 3, 10)
        assert deleted

    def test_update_not_allowed_when_not_ready(self, emrserverless_client):
        """Test that updating an application while it is not in CREATED or
        STOPPED state sets Synced=False with the correct message and requeues
        until the application returns to a modifiable state."""
        resource_name = random_suffix_name("ack-test-app-upd", 24)

        (ref, cr) = _create_application(resource_name)

        assert cr is not None
        time.sleep(CREATE_WAIT_AFTER_SECONDS)
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)

        cr = k8s.get_resource(ref)
        application_id = cr["status"]["id"]

        # Start the application via boto3 to move it out of CREATED/STOPPED
        emrserverless_client.start_application(applicationId=application_id)

        # Wait until the application is no longer in CREATED or STOPPED
        for _ in range(30):
            time.sleep(5)
            resp = emrserverless_client.get_application(applicationId=application_id)
            state = resp["application"]["state"]
            if state not in ("CREATED", "STOPPED"):
                break

        # Patch a spec field to trigger an update while the app is not ready
        k8s.patch_custom_resource(ref, {
            "spec": {
                "autoStopConfiguration": {
                    "idleTimeoutMinutes": 25
                }
            }
        })
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        # Verify the Synced condition is False with the expected message
        condition.assert_synced_status(ref, False)
        cond = k8s.get_resource_condition(ref, "ACK.ResourceSynced")
        assert cond is not None
        assert "message" in cond
        assert "cannot be modified" in cond["message"], (
            f"Expected message to mention cannot be modified but got: {cond['message']}"
        )

        # Stop the application so it returns to a modifiable state
        try:
            emrserverless_client.stop_application(applicationId=application_id)
        except Exception:
            pass

        # Wait for the application to reach STOPPED or CREATED
        for _ in range(30):
            time.sleep(10)
            resp = emrserverless_client.get_application(applicationId=application_id)
            state = resp["application"]["state"]
            if state in ("CREATED", "STOPPED"):
                break

        # The controller should eventually reconcile and sync successfully
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=15)

        # Cleanup
        _, deleted = k8s.delete_custom_resource(ref, 3, 10)
        assert deleted

