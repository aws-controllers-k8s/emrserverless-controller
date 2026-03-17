if !applicationCreatedOrStopped(latest) {
    msg := fmt.Sprintf("application in '%s' state, cannot be modified until 'CREATED' or 'STOPPED'", string(*latest.ko.Status.State))
    ackcondition.SetSynced(latest, corev1.ConditionFalse, &msg, nil)
    return latest, requeueWaitUntilCanModify(latest)
}
updatedDesired := desired.DeepCopy()
updatedDesired.SetStatus(latest)
if delta.DifferentAt("Spec.Tags") {
    arn := string(*latest.ko.Status.ACKResourceMetadata.ARN)
    err = syncTags(
        ctx, 
        desired.ko.Spec.Tags, latest.ko.Spec.Tags, 
        &arn, convertToOrderedACKTags, rm.sdkapi, rm.metrics,
    )
    if err != nil {
        return nil, err
    }
}
if !delta.DifferentExcept("Spec.Tags") {
    return rm.concreteResource(updatedDesired), nil
}
