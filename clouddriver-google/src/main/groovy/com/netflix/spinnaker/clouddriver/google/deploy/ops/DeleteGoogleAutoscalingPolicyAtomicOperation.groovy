/*
 * Copyright 2016 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.clouddriver.google.deploy.ops

import com.google.api.services.compute.Compute
import com.google.api.services.compute.model.Autoscaler
import com.google.api.services.compute.model.InstanceGroupManager
import com.google.api.services.compute.model.InstanceGroupManagersSetAutoHealingRequest
import com.google.api.services.compute.model.InstanceTemplate
import com.google.api.services.compute.model.RegionInstanceGroupManagersSetAutoHealingRequest
import com.netflix.spinnaker.clouddriver.data.task.Task
import com.netflix.spinnaker.clouddriver.data.task.TaskRepository
import com.netflix.spinnaker.clouddriver.google.compute.RegionAutoscalers
import com.netflix.spinnaker.clouddriver.google.deploy.GCEUtil
import com.netflix.spinnaker.clouddriver.google.deploy.GoogleOperationPoller
import com.netflix.spinnaker.clouddriver.google.deploy.description.DeleteGoogleAutoscalingPolicyDescription
import com.netflix.spinnaker.clouddriver.google.model.GoogleServerGroup
import com.netflix.spinnaker.clouddriver.google.model.callbacks.Utils
import com.netflix.spinnaker.clouddriver.google.provider.view.GoogleClusterProvider
import com.netflix.spinnaker.clouddriver.google.security.GoogleNamedAccountCredentials
import com.netflix.spinnaker.clouddriver.orchestration.AtomicOperation
import com.netflix.spinnaker.clouddriver.orchestration.AtomicOperationsRegistry
import com.netflix.spinnaker.clouddriver.orchestration.OrchestrationProcessor
import org.springframework.beans.factory.annotation.Autowired
import com.google.api.services.compute.Compute.RegionAutoscalers
import com.google.api.services.compute.Compute.Autoscalers

class DeleteGoogleAutoscalingPolicyAtomicOperation extends GoogleAtomicOperation<Void>{

  private static final String BASE_PHASE = "DELETE_SCALING_POLICY"
  private final DeleteGoogleAutoscalingPolicyDescription description

  @Autowired
  private GoogleClusterProvider googleClusterProvider

  @Autowired
  private GoogleOperationPoller googleOperationPoller

  @Autowired
  AtomicOperationsRegistry atomicOperationsRegistry

  @Autowired
  OrchestrationProcessor orchestrationProcessor

  private static Task getTask() {
    TaskRepository.threadLocalTask.get()
  }

  DeleteGoogleAutoscalingPolicyAtomicOperation(DeleteGoogleAutoscalingPolicyDescription description) {
    this.description = description
  }

  /**
   * Autoscaling policy:
   * curl -X POST -H "Content-Type: application/json" -d '[ { "deleteScalingPolicy": { "serverGroupName": "autoscale-regional", "credentials": "my-google-account", "region": "us-central1" }} ]' localhost:7002/gce/ops
   * curl -X POST -H "Content-Type: application/json" -d '[ { "deleteScalingPolicy": { "serverGroupName": "autoscale-zonal", "credentials": "my-google-account", "region": "us-central1" }} ]' localhost:7002/gce/ops
   *
   * AutoHealing policy:
   * curl -X POST -H "Content-Type: application/json" -d '[ { "deleteScalingPolicy": { "serverGroupName": "autoscale-zonal", "credentials": "my-google-account", "region": "us-central1", "deleteAutoHealingPolicy": true }} ]' localhost:7002/gce/ops
   */
  @Override
  Void operate(List priorOutputs) {
    def credentials = description.credentials
    def serverGroupName = description.serverGroupName
    def project = credentials.project
    def compute = credentials.compute
    def accountName = description.accountName
    def region = description.region
    def serverGroup = GCEUtil.queryServerGroup(googleClusterProvider, accountName, region, serverGroupName)
    def isRegional = serverGroup.regional
    def zone = serverGroup.zone

    if (description.deleteAutoHealingPolicy) {
      task.updateStatus BASE_PHASE, "Initializing deletion of autoHealing policy for $description.serverGroupName..."
      if (isRegional) {
        def request = new RegionInstanceGroupManagersSetAutoHealingRequest().setAutoHealingPolicies([])
        def deleteOp = timeExecute(
          compute.regionInstanceGroupManagers().setAutoHealingPolicies(project, region, serverGroupName, request),
          "compute.regionInstanceGroupManagers.setAutoHealingPolicies",
          TAG_SCOPE, SCOPE_REGIONAL, TAG_REGION, region)
        googleOperationPoller.waitForRegionalOperation(compute, project, region,
          "autoHealingOp", null, task, "autoHealing policy for $serverGroupName", BASE_PHASE)
        deletePolicyMetadata(compute, credentials, project, GCEUtil.buildRegionalServerGroupUrl(project, region, serverGroupName))
      } else {
        def request = new InstanceGroupManagersSetAutoHealingRequest().setAutoHealingPolicies([])
         def deleteOp = timeExecute(
          compute.instanceGroupManagers().setAutoHealingPolicies(project, zone, serverGroupName, request),
          "compute.instanceGroupManagers.setAutoHealingPolicies",
          TAG_SCOPE, SCOPE_ZONAL, TAG_ZONE, zone)
        googleOperationPoller.waitForZonalOperation(compute, project, zone,
          "autoHealingOp", null, task, "autoHealing policy for $serverGroupName", BASE_PHASE)
         deletePolicyMetadata(compute, credentials, project, GCEUtil.buildZonalServerGroupUrl(project, zone, serverGroupName))
      }
      task.updateStatus BASE_PHASE, "Done deleting autoHealing policy for $serverGroupName."
    } else {
      task.updateStatus BASE_PHASE, "Initializing deletion of scaling policy for $description.serverGroupName..."
      if (isRegional) {
         def deleteOp = timeExecute(
           compute.regionInstanceGroupManagers().delete(project, region, serverGroupName ),
          "compute.regionInstanceGroupManagers.delete",
          TAG_SCOPE, SCOPE_REGIONAL, TAG_REGION, region)
        googleOperationPoller.waitForRegionalOperation(compute, project, region,
          "deleteOp", null, task, "autoScaling policy for $serverGroupName", BASE_PHASE)
        deletePolicyMetadata(compute, credentials, project, GCEUtil.buildRegionalServerGroupUrl(project, region, serverGroupName))
      } else {
        def deleteOp = timeExecute(
          compute.instanceGroupManagers().delete(project, zone, serverGroupName ),
          "compute.instanceGroupManagers.delete",
          TAG_SCOPE, SCOPE_REGIONAL, TAG_REGION, zone)
        googleOperationPoller.waitForRegionalOperation(compute, project, zone,
          "deleteOp", null, task, "autoScaling policy for $serverGroupName", BASE_PHASE)
        deletePolicyMetadata(compute, credentials, project, GCEUtil.buildZonalServerGroupUrl(project, zone, serverGroupName))
      }
      task.updateStatus BASE_PHASE, "Done deleting scaling policy for $serverGroupName."
    }

    return null
  }


  void deletePolicyMetadata(Compute compute,
                            GoogleNamedAccountCredentials credentials,
                            String project,
                            String groupUrl) {
    def groupName = Utils.getLocalName(groupUrl)
    def groupRegion = Utils.getRegionFromGroupUrl(groupUrl)

    String templateUrl = null
    switch (Utils.determineServerGroupType(groupUrl)) {
      case GoogleServerGroup.ServerGroupType.REGIONAL:
         Compute.RegionInstanceGroupManagers.Get getOperation = compute.regionInstanceGroupManagers().get(project, groupRegion, groupName)
        if(getOperation != null){
          def regionInstanceGroupManager = timeExecute(
            getOperation,
            "compute.regionInstanceGroupManagers.get",
            TAG_SCOPE, SCOPE_REGIONAL, TAG_REGION, groupRegion)
          if(regionInstanceGroupManager != null) {
            templateUrl = regionInstanceGroupManager.getInstanceTemplate()
          }else {
            println( "timeExecute for compute.regionInstanceGroupManagers().get(${project}, ${groupRegion}, ${groupName}) " +
              "returned null in the specified zone and project.")
          }
        }else {
          println( "compute.regionInstanceGroupManagers().get(${project}, ${groupRegion}, ${groupName}) " +
            "returned null in the specified zone and project.")
        }
        break
      case GoogleServerGroup.ServerGroupType.ZONAL:
        def groupZone = Utils.getZoneFromGroupUrl(groupUrl)
         Compute.InstanceGroupManagers.Get getOperation = compute.instanceGroupManagers().get(project, groupZone, groupName)
        if(getOperation != null) {
           def instanceGroupManager = timeExecute(
            getOperation,
            "compute.instanceGroupManagers.get",
            TAG_SCOPE, SCOPE_ZONAL, TAG_ZONE, groupZone
          )
          if(instanceGroupManager != null){
            templateUrl = instanceGroupManager.getInstanceTemplate()
          }else {
            println( "timeExecute for compute.instanceGroupManagers().get(${project}, ${groupZone}, ${groupName}) " +
              "returned null in the specified zone and project.")
          }

         }else {
          println( "operation compute.instanceGroupManagers().get(${project}, ${groupZone}, ${groupName}) " +
            "returned null in the specified zone and project.")
        }
         break
      default:
        throw new IllegalStateException("Server group referenced by ${groupUrl} has illegal type.")
        break
    }

    if(templateUrl != null){
      InstanceTemplate template = timeExecute(
        compute.instanceTemplates().get(project, Utils.getLocalName(templateUrl)),
        "compute.instancesTemplates.get",
        TAG_SCOPE, SCOPE_GLOBAL)
      def instanceDescription = GCEUtil.buildInstanceDescriptionFromTemplate(project, template)

      def templateOpMap = [
        image              : instanceDescription.image,
        instanceType       : instanceDescription.instanceType,
        credentials        : credentials.getName(),
        disks              : instanceDescription.disks,
        instanceMetadata   : instanceDescription.instanceMetadata,
        tags               : instanceDescription.tags,
        network            : instanceDescription.network,
        subnet             : instanceDescription.subnet,
        serviceAccountEmail: instanceDescription.serviceAccountEmail,
        authScopes         : instanceDescription.authScopes,
        preemptible        : instanceDescription.preemptible,
        automaticRestart   : instanceDescription.automaticRestart,
        onHostMaintenance  : instanceDescription.onHostMaintenance,
        region             : groupRegion,
        serverGroupName    : groupName
      ]

      if (instanceDescription.minCpuPlatform) {
        templateOpMap.minCpuPlatform = instanceDescription.minCpuPlatform
      }

      if (templateOpMap?.instanceMetadata) {
        templateOpMap.instanceMetadata.remove(GCEUtil.AUTOSCALING_POLICY)
        def converter = atomicOperationsRegistry.getAtomicOperationConverter('modifyGoogleServerGroupInstanceTemplateDescription', 'gce')
        AtomicOperation templateOp = converter.convertOperation(templateOpMap)
        orchestrationProcessor.process('gce', [templateOp], UUID.randomUUID().toString())
      }
    }else {
      println( " templateUrl ${templateUrl} not found in the specified region and project.")
    }
  }

}
