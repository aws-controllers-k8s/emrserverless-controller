// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package application

import (
	"fmt"

	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"

	svcapitypes "github.com/aws-controllers-k8s/emrserverless-controller/apis/v1alpha1"
	"github.com/aws-controllers-k8s/emrserverless-controller/pkg/sync"
)

var syncTags = sync.Tags

// applicationCreatedOrStopped returns true if the supplied application is in
// a CREATED or STOPPED state, which are the only states that allow updates.
func applicationCreatedOrStopped(r *resource) bool {
	if r.ko.Status.State == nil {
		return false
	}
	state := *r.ko.Status.State
	return state == string(svcapitypes.ApplicationState_CREATED) ||
		state == string(svcapitypes.ApplicationState_STOPPED)
}

// requeueWaitUntilCanModify returns a `ackrequeue.RequeueNeededAfter` struct
// explaining the application cannot be modified until it reaches a CREATED or
// STOPPED state.
func requeueWaitUntilCanModify(r *resource) *ackrequeue.RequeueNeededAfter {
	if r.ko.Status.State == nil {
		return nil
	}
	state := *r.ko.Status.State
	return ackrequeue.NeededAfter(
		fmt.Errorf("application in '%s' state, cannot be modified until 'CREATED' or 'STOPPED'", state),
		ackrequeue.DefaultRequeueAfterDuration,
	)
}
