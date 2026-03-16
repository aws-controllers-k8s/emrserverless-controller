	if ko.Status.State != nil && *ko.Status.State == string(svcapitypes.ApplicationState_TERMINATED) {
		return nil, ackerr.NotFound
	}
