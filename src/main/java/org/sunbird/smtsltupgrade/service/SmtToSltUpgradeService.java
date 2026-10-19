package org.sunbird.smtsltupgrade.service;

import org.sunbird.common.model.SBApiResponse;

public interface SmtToSltUpgradeService {

    /**
     * Scans all users currently holding the SMT role, computes each one's completed-batch count
     * and total learners trained (across batches where they're the creator, mentor, or co-trainer),
     * and upgrades any user crossing either threshold to SLT.
     *
     * Intended to be triggered by an external Kubernetes CronJob hitting this service's controller
     * endpoint on a schedule - this method itself does not schedule anything.
     */
    SBApiResponse runSmtToSltUpgradeCheck();
}
