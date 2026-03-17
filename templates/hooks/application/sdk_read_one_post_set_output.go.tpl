	// The EMR Serverless GetApplication API continues to return deleted
	// applications with their state set to TERMINATED rather than returning
	// a NotFound error. 
	//
	// Treating TERMINATED as NotFound allows the reconciler to proceed with
	// Create if user deletes application externally
	if ko.Status.State != nil && *ko.Status.State == string(svcapitypes.ApplicationState_TERMINATED) {
		return nil, ackerr.NotFound
	}
