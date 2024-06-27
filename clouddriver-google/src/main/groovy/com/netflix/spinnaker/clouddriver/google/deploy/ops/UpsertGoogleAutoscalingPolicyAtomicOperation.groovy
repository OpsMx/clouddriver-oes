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

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.api.services.compute.Compute
import com.google.api.services.compute.model.*
import com.google.common.annotations.VisibleForTesting
import com.netflix.spinnaker.cats.cache.Cache
import com.netflix.spinnaker.clouddriver.data.task.Task
import com.netflix.spinnaker.clouddriver.data.task.TaskRepository
import com.netflix.spinnaker.clouddriver.google.deploy.GCEUtil
import com.netflix.spinnaker.clouddriver.google.deploy.GoogleOperationPoller
import com.netflix.spinnaker.clouddriver.google.deploy.description.UpsertGoogleAutoscalingPolicyDescription
import com.netflix.spinnaker.clouddriver.google.model.GoogleAutoHealingPolicy
import com.netflix.spinnaker.clouddriver.google.model.GoogleAutoscalingPolicy
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
import com.google.api.services.compute.Compute.RegionInstanceGroupManagers

class UpsertGoogleAutoscalingPolicyAtomicOperation extends GoogleAtomicOperation<Void> {
  private static final String BASE_PHASE = "UPSERT_SCALING_POLICY"

  private static Task getTask() {
    TaskRepository.threadLocalTask.get()
  }

  @Autowired
  private GoogleClusterProvider googleClusterProvider

  @Autowired
  private GoogleOperationPoller googleOperationPoller

  @Autowired
  AtomicOperationsRegistry atomicOperationsRegistry

  @Autowired
  OrchestrationProcessor orchestrationProcessor

  @Autowired
  Cache cacheView

  @Autowired
  ObjectMapper objectMapper

/*
  @Autowired
  com.google.api.services.compute.model.InstanceGroupManager content
*/

  private final UpsertGoogleAutoscalingPolicyDescription description

  UpsertGoogleAutoscalingPolicyAtomicOperation(UpsertGoogleAutoscalingPolicyDescription description) {
    this.description = description
  }

  /**
   * Autoscaling policy:
   * curl -X POST -H "Content-Type: application/json" -d '[ { "upsertScalingPolicy": { "serverGroupName": "autoscale-regional", "region": "us-central1", "credentials": "my-google-account", "autoscalingPolicy": { "maxNumReplicas": 2, "minNumReplicas": 1, "coolDownPeriodSec" : 30, "cpuUtilization": { "utilizationTarget": 0.7 }}}} ]' localhost:7002/gce/ops
   * curl -X POST -H "Content-Type: application/json" -d '[ { "upsertScalingPolicy": { "serverGroupName": "autoscale-regional", "region": "us-central1", "credentials": "my-google-account", "autoscalingPolicy": { "cpuUtilization": { "utilizationTarget": 0.5 }}}} ]' localhost:7002/gce/ops
   * curl -X POST -H "Content-Type: application/json" -d '[ { "upsertScalingPolicy": { "serverGroupName": "autoscale-regional", "region": "us-central1", "credentials": "my-google-account", "autoscalingPolicy": { "maxNumReplicas": 2, "loadBalancingUtilization": { "utilizationTarget": 0.7 }, "cpuUtilization": {}}}} ]' localhost:7002/gce/ops
   * curl -X POST -H "Content-Type: application/json" -d '[ { "upsertScalingPolicy": { "serverGroupName": "autoscale-regional", "region": "us-central1", "credentials": "my-google-account", "autoscalingPolicy": { "maxNumReplicas": 3, "minNumReplicas": 2 , "coolDownPeriodSec" : 60 }}} ]' localhost:7002/gce/ops
   * curl -X POST -H "Content-Type: application/json" -d '[ { "upsertScalingPolicy": { "serverGroupName": "autoscale-regional", "region": "us-central1", "credentials": "my-google-account", "autoscalingPolicy": { "coolDownPeriodSec": 35, "cpuUtilization": { "utilizationTarget": 0.9 }, "loadBalancingUtilization": { "utilizationTarget" : 0.6 }, "customMetricUtilizations" : [ { "metric": "myMetric", "utilizationTarget": 0.9, "utilizationTargetType" : "DELTA_PER_SECOND" } ] }}} ]' localhost:7002/gce/ops
   * curl -X POST -H "Content-Type: application/json" -d '[ { "upsertScalingPolicy": { "serverGroupName": "autoscale-regional", "region": "us-central1", "credentials": "my-google-account", "autoscalingPolicy": { "maxNumReplicas": 2, "minNumReplicas": 1, "coolDownPeriodSec": 30, "cpuUtilization": {}, "loadBalancingUtilization": {}, "customMetricUtilizations" : [] }}} ]' localhost:7002/gce/ops
   *
   * Autohealing policy:
   * curl -X POST -H "Content-Type: application/json" -d '[ { "upsertScalingPolicy": { "serverGroupName": "autoheal-regional", "region": "us-central1", "credentials": "my-google-account", "autoHealingPolicy": {"initialDelaySec": 30, "healthCheck": "hc", "maxUnavailable": { "fixed": 3 }}}} ]' localhost:7002/gce/ops
   * curl -X POST -H "Content-Type: application/json" -d '[ { "upsertScalingPolicy": { "serverGroupName": "autoheal-regional", "region": "us-central1", "credentials": "my-google-account", "autoHealingPolicy": {"initialDelaySec": 50}}} ]' localhost:7002/gce/ops
   */

  @Override
  Void operate(List priorOutputs) {
    task.updateStatus BASE_PHASE, "Initializing upsert of scaling policy for $description.serverGroupName..."

    def serverGroupName = description.serverGroupName
    def credentials = description.credentials
    def project = credentials.project
    def compute = credentials.compute
    def accountName = description.accountName
    def region = description.region
    def serverGroup = GCEUtil.queryServerGroup(googleClusterProvider, accountName, region, serverGroupName)
    def isRegional = serverGroup.regional
    def zone = serverGroup.zone
    def instanceGroupManager ="my-instance-group"
    def autoscaler = null
    if (description.autoscalingPolicy) {
      def ancestorAutoscalingPolicyDescription = serverGroup.autoscalingPolicy
      if (ancestorAutoscalingPolicyDescription) {
        task.updateStatus BASE_PHASE, "Updating autoscaler for $serverGroupName..."

        autoscaler = GCEUtil.buildAutoscaler(serverGroupName,
          serverGroup.selfLink,
          copyAndOverrideAncestorAutoscalingPolicy(ancestorAutoscalingPolicyDescription,
            description.autoscalingPolicy))

        if (isRegional) {
          def updateOp = timeExecute(
            compute.regionAutoscalers().update(project, region, autoscaler),
            "compute.regionAutoscalers.update",
            TAG_SCOPE, SCOPE_REGIONAL, TAG_REGION, region)
          googleOperationPoller.waitForRegionalOperation(compute, project, region,
            "updateOp", null, task, "autoScaler ${autoscaler.getName()} for server group $serverGroupName", BASE_PHASE)
        } else {
          def updateOp = timeExecute(
            compute.autoscalers().update(project, zone, autoscaler),
            "compute.autoscalers.update",
            TAG_SCOPE, SCOPE_ZONAL, TAG_ZONE, zone)
          googleOperationPoller.waitForZonalOperation(compute, project, zone,
            "updateOp", null, task, "autoScaler ${autoscaler.getName()} for server group $serverGroupName", BASE_PHASE)
        }
      } else {
        task.updateStatus BASE_PHASE, "Creating new autoscaler for $serverGroupName..."

        autoscaler = GCEUtil.buildAutoscaler(serverGroupName,
          serverGroup.selfLink,
          normalizeNewAutoscalingPolicy(description.autoscalingPolicy))

        if (isRegional) {
          def insertOp = timeExecute(
            compute.regionAutoscalers().insert(project, region, autoscaler),
            "compute.regionAutoscalers.insert",
            TAG_SCOPE, SCOPE_REGIONAL, TAG_REGION, region)
          googleOperationPoller.waitForRegionalOperation(compute, project, region,
            "insertOp", null, task, "autoScaler ${autoscaler.getName()} for server group $serverGroupName", BASE_PHASE)
        } else {
          def insertOp = timeExecute(
            compute.autoscalers().insert(project, zone, autoscaler),
            "compute.autoscalers.insert",
            TAG_SCOPE, SCOPE_ZONAL, TAG_ZONE, zone)
          googleOperationPoller.waitForZonalOperation(compute, project, zone,
            "insertOp", null, task, "autoScaler ${autoscaler.getName()} for server group $serverGroupName", BASE_PHASE)
        }
      }
    }

    if (description.autoHealingPolicy) {
      def ancestorAutoHealingPolicyDescription =
        GCEUtil.buildAutoHealingPolicyDescriptionFromAutoHealingPolicy(serverGroup.autoHealingPolicy)

      def regionalRequest = { List<InstanceGroupManagerAutoHealingPolicy> policy ->
        def request = new RegionInstanceGroupManagersSetAutoHealingRequest().setAutoHealingPolicies(policy)
        def autoHealingOp = timeExecute(
          compute.regionInstanceGroupManagers().setAutoHealingPolicies(project, region, serverGroupName, request),
          "compute.regionInstanceGroupManagers.setAutoHealingPolicies",
          TAG_SCOPE, SCOPE_REGIONAL, TAG_REGION, region)
        googleOperationPoller.waitForRegionalOperation(compute, project, region,
          autoHealingOp.getName(), null, task, "autoHealing policy ${policy} for server group $serverGroupName", BASE_PHASE)
      }

      def zonalRequest = { List<InstanceGroupManagerAutoHealingPolicy> policy ->
        def request = new InstanceGroupManagersSetAutoHealingRequest().setAutoHealingPolicies(policy)
        def autoHealingOp = timeExecute(
          compute.instanceGroupManagers().setAutoHealingPolicies(project, zone, serverGroupName, request),
          "compute.instanceGroupManagers.setAutoHealingPolicies",
          TAG_SCOPE, SCOPE_ZONAL, TAG_ZONE, zone)
        googleOperationPoller.waitForZonalOperation(compute, project, zone,
          autoHealingOp.getName(), null, task, "autoHealing policy ${policy} for server group $serverGroupName", BASE_PHASE)
      }

      if (ancestorAutoHealingPolicyDescription) {
        task.updateStatus BASE_PHASE, "Updating autoHealing policy for $serverGroupName..."

        def autoHealingPolicy =
          buildAutoHealingPolicyFromAutoHealingPolicyDescription(
            copyAndOverrideAncestorAutoHealingPolicy(ancestorAutoHealingPolicyDescription, description.autoHealingPolicy),
            project, compute)
        isRegional ? regionalRequest(autoHealingPolicy) : zonalRequest(autoHealingPolicy)

      } else {
        task.updateStatus BASE_PHASE, "Creating new autoHealing policy for $serverGroupName..."

        def autoHealingPolicy =
          buildAutoHealingPolicyFromAutoHealingPolicyDescription(
            normalizeNewAutoHealingPolicy(description.autoHealingPolicy),
            project, compute)
        isRegional ? regionalRequest(autoHealingPolicy) : zonalRequest(autoHealingPolicy)
      }
    }

    // TODO(jacobkiefer): Update metadata for autoHealingPolicy when 'mode' support lands.
    // NOTE: This block is here intentionally, we should wait until all the modifications are done before
    // updating the instance template metadata.
    if (description.writeMetadata == null || description.writeMetadata) {
      if (isRegional) {
        updatePolicyMetadata(compute,
          credentials,
          project,
          GCEUtil.buildRegionalServerGroupUrl(project, region, serverGroupName),
          autoscaler)
      } else {
        updatePolicyMetadata(compute,
          credentials,
          project,
          GCEUtil.buildZonalServerGroupUrl(project, zone, serverGroupName),
          autoscaler)
      }
    }

    return null
  }

  private static GoogleAutoscalingPolicy copyAndOverrideAncestorAutoscalingPolicy(GoogleAutoscalingPolicy ancestor,
                                                                                  GoogleAutoscalingPolicy update) {
    GoogleAutoscalingPolicy newDescription = ancestor.clone()

    if (!update) {
      return newDescription
    }

    // Deletes existing customMetricUtilizations if passed an empty array.
    ["minNumReplicas", "maxNumReplicas", "coolDownPeriodSec", "customMetricUtilizations", "mode", "scalingSchedules"].each {
      if (update[it] != null) {
        newDescription[it] = update[it]
      }
    }

    // If scaleInControl is completely absent, we leave the previous value.
    // To remove it, set it to an empty object.
    if (update.scaleInControl != null) {
      def scaleInControl = update.scaleInControl
      if (scaleInControl.timeWindowSec != null && scaleInControl.maxScaledInReplicas != null) {
        newDescription.scaleInControl = scaleInControl
      } else {
        newDescription.scaleInControl = null
      }
    }

    // Deletes existing cpuUtilization or loadBalancingUtilization if passed an empty object.
    ["cpuUtilization", "loadBalancingUtilization"].each {
      if (update[it] != null) {
        if (update[it].utilizationTarget != null) {
          newDescription[it] = update[it]
        } else {
          newDescription[it] = null
        }
      }
    }

    return newDescription
  }

  // Forces the behavior of this operation to be consistent: passing an empty `cpuUtilization` or
  // `loadBalancingUtilization` object always results in a policy without these properties.
  private static GoogleAutoscalingPolicy normalizeNewAutoscalingPolicy(GoogleAutoscalingPolicy newPolicy) {
    ["cpuUtilization", "loadBalancingUtilization"].each {
      if (newPolicy[it]?.utilizationTarget == null) {
        newPolicy[it] = null
      }
    }

    return newPolicy
  }

  @VisibleForTesting
  static GoogleAutoHealingPolicy copyAndOverrideAncestorAutoHealingPolicy(GoogleAutoHealingPolicy ancestor,
                                                                          GoogleAutoHealingPolicy update) {
    GoogleAutoHealingPolicy newDescription = ancestor.clone()

    if (!update) {
      return newDescription
    }

    ["healthCheck", "initialDelaySec", "healthCheckKind"].each {
      if (update[it] != null) {
        newDescription[it] = update[it]
      }
    }

    // Deletes existing maxUnavailable if passed an empty object.
    if (update.maxUnavailable != null) {
      if (update.maxUnavailable.fixed != null || update.maxUnavailable.percent != null) {
        newDescription.maxUnavailable = update.maxUnavailable
      } else {
        newDescription.maxUnavailable = null
      }
    }

    return newDescription
  }

  // Forces the behavior of this operation to be consistent: passing an empty `maxUnavailable` object
  // always results in a policy with no `maxUnavailable` property.
  private static GoogleAutoHealingPolicy normalizeNewAutoHealingPolicy(GoogleAutoHealingPolicy newPolicy) {
    if (newPolicy.maxUnavailable?.fixed == null && newPolicy.maxUnavailable?.percent == null) {
      newPolicy.maxUnavailable = null
    }

    return newPolicy
  }

  private buildAutoHealingPolicyFromAutoHealingPolicyDescription(GoogleAutoHealingPolicy autoHealingPolicyDescription, String project, Compute compute) {
    def autoHealingHealthCheck = GCEUtil.queryHealthCheck(project, description.accountName, autoHealingPolicyDescription.healthCheck, autoHealingPolicyDescription.healthCheckKind, compute, cacheView, task, BASE_PHASE, this)

    List<InstanceGroupManagerAutoHealingPolicy> autoHealingPolicy = autoHealingPolicyDescription?.healthCheck
      ? [new InstanceGroupManagerAutoHealingPolicy(
      healthCheck: autoHealingHealthCheck.selfLink,
      initialDelaySec: autoHealingPolicyDescription.initialDelaySec)]
      : null

    if (autoHealingPolicy && autoHealingPolicyDescription.maxUnavailable) {
      def maxUnavailable = new FixedOrPercent(fixed: autoHealingPolicyDescription.maxUnavailable.fixed as Integer,
        percent: autoHealingPolicyDescription.maxUnavailable.percent as Integer)

      autoHealingPolicy[0].setMaxUnavailable(maxUnavailable)
    }

    return autoHealingPolicy
  }
/*

  void updatePolicyMetadata2(Compute compute,
                            GoogleNamedAccountCredentials credentials,
                            String project,
                            String groupUrl,
                            autoscaler) {
    def groupName = Utils.getLocalName(groupUrl)
    String templateUrl = null
    //def instanceGroupManager ="my-instance-group"
    def groupRegion = Utils.getRegionFromGroupUrl(groupUrl)

    switch (Utils.determineServerGroupType(groupUrl)) {
      case GoogleServerGroup.ServerGroupType.REGIONAL:

        */
/*templateUrl = timeExecute(
          compute.regionInstanceGroupManagers().get(project, groupRegion, groupName),
          "compute.regionInstanceGroupManagers.get",
          TAG_SCOPE, SCOPE_REGIONAL, TAG_REGION, groupRegion)
          .getInstanceTemplate()*//*


        */
/*   println("DEBUG1-get : [REGIONAL] : [compute=${compute}] : [regionInstanceGroupManagers()=${compute.regionInstanceGroupManagers()}]")
        println("DEBUG1-get : [REGIONAL] : [project=${project}] : [groupRegion=${groupRegion}] : [instanceGroupManager=${instanceGroupManager}]")
        println("DEBUG1-get : [REGIONAL] : [TAG_SCOPE=${TAG_SCOPE}] : [SCOPE_REGIONAL=${SCOPE_REGIONAL}] :" +
          " [TAG_REGION=${TAG_REGION}] : [groupRegion=${groupRegion}]")
        RegionInstanceGroupManagers regionAutoscalers = compute.regionInstanceGroupManagers()
        def regionAutoscalerList = regionAutoscalers.get(project, groupRegion, instanceGroupManager).execute()*//*


*/
/*
        println("DEBUG1-get : [REGIONAL] : [compute=${compute}] : [regionAutoscalers()=${compute.regionAutoscalers()}]")
        println("DEBUG1-get : [REGIONAL] : [project=${project}] : [groupRegion=${groupRegion}] : [groupName=${groupName}]")
        println("DEBUG1-get : [REGIONAL] : [TAG_SCOPE=${TAG_SCOPE}] : [SCOPE_REGIONAL=${SCOPE_REGIONAL}] : [TAG_REGION=${TAG_REGION}] : [groupRegion=${groupRegion}]")

        com.google.api.services.compute.Compute.RegionAutoscalers regionAutoscalers = compute.regionAutoscalers()
        def regionAutoscalerList = regionAutoscalers.get(project, groupRegion, groupName).execute()

        if (regionAutoscalerList != null) {
          templateUrl = regionAutoscalerList.getInstanceTemplate()
          println "DEBUG1-get : [REGIONAL] : [regionAutoscalers Name: ${regionAutoscalerList.getName()}]"
          println "DEBUG1-get : [REGIONAL] : [regionAutoscalers Target: ${regionAutoscalerList.getTarget()}]"
          println "DEBUG1-get : [REGIONAL] : [Instance Template Link: ${templateUrl}]"
        } else {
          println "DEBUG1-get : [REGIONAL] : [regionAutoscalers ${regionAutoscalerList} not found in the specified region and project.]"
        }*//*


        println("DEBUG1-get : [REGIONAL] : [compute=${compute}] : [regionAutoscalers()=${compute.regionAutoscalers()}]")
        println("DEBUG1-get : [REGIONAL] : [project=${project}] : [groupRegion=${groupRegion}] : [groupName=${groupName}]")
        println("DEBUG1-get : [REGIONAL] : [TAG_SCOPE=${TAG_SCOPE}] : [SCOPE_REGIONAL=${SCOPE_REGIONAL}] : [TAG_REGION=${TAG_REGION}] : [groupRegion=${groupRegion}]")
        com.google.api.services.compute.Compute.RegionAutoscalers regionAutoscalers = compute.regionAutoscalers()
        Compute.RegionAutoscalers.Get getOperation = regionAutoscalers.get(project, groupRegion, groupName)
        if (getOperation != null) {
          def regionInstanceGroupManagerList = getOperation.execute()

          if (regionInstanceGroupManagerList != null) {
            templateUrl = regionInstanceGroupManagerList.getInstanceTemplate()
            println "DEBUG1-get : [REGIONAL] : [regionAutoscalerList Name: ${regionInstanceGroupManagerList.getName()}]"
            println "DEBUG1-get : [REGIONAL] : [regionAutoscalerList Target: ${regionInstanceGroupManagerList.getTarget()}]"
            println "DEBUG1-get : [REGIONAL] : [Instance Template Link: ${templateUrl}]"
          } else {
            println "DEBUG1-get : [REGIONAL] : [regionAutoscalerList ${regionInstanceGroupManagerList} not found in the specified region and project.]"
          }
        } else {
          println "DEBUG1-get : [REGIONAL] : [getOperation ${getOperation} not found in the specified region and project.]"
        }
        break
      case GoogleServerGroup.ServerGroupType.ZONAL:
        def groupZone = Utils.getZoneFromGroupUrl(groupUrl)
        */
/*templateUrl = timeExecute(
          compute.instanceGroupManagers().get(project, groupZone, groupName),
          "compute.instanceGroupManagers.get",
          TAG_SCOPE, SCOPE_ZONAL, TAG_ZONE, groupZone)
          .getInstanceTemplate()*//*

        println("DEBUG2-get : [ZONAL] : [compute=${compute}] : [autoscalers()=${compute.autoscalers()}]")
        println("DEBUG2-get : [ZONAL] : [project=${project}] : [groupZone=${groupZone}] : [groupName=${groupName}]")
        println("DEBUG2-get : [ZONAL] : [TAG_SCOPE=${TAG_SCOPE}] : [SCOPE_ZONAL=${SCOPE_ZONAL}] : [TAG_ZONE=${TAG_ZONE}] : [groupZone=${groupZone}]")

        com.google.api.services.compute.Compute.Autoscalers autoscalers = compute.autoscalers()
        Compute.Autoscalers.Get getOperation = autoscalers.get(project, groupZone, groupName)
        if (getOperation != null) {
          def instanceGroupManagerList = getOperation.execute()

          if (instanceGroupManagerList != null) {
            templateUrl = instanceGroupManagerList.getInstanceTemplate()
            println "DEBUG2-get : [ZONAL] : [Autoscalers Name: ${instanceGroupManagerList.getName()}]"
            println "DEBUG2-get : [ZONAL] : [Autoscalers Target: ${instanceGroupManagerList.getTarget()}]"
            println "DEBUG2-get : [ZONAL] : [Instance Template Link: ${templateUrl}]"
          } else {
            println "DEBUG2-get : [ZONAL] : [Autoscalers ${instanceGroupManagerList} not found in the specified region and project.]"
          }
        } else {
          println "DEBUG2-get : [ZONAL] : [getOperation ${getOperation} not found in the specified region and project.]"
        }
        break
      default:
        throw new IllegalStateException("Server group referenced by ${groupUrl} has illegal type.")
        break
    }

    if (templateUrl != null) {
      */
/*InstanceTemplate template = timeExecute(
        compute.instanceTemplates().get(project, Utils.getLocalName(templateUrl)),
        "compute.instancesTemplates.get",
        TAG_SCOPE, SCOPE_GLOBAL)*//*

      def instanceTemplateName = "your_instance_template_name"

      // Call the get() method to retrieve the instance template
      InstanceTemplate template = compute.instanceTemplates().get(project, instanceTemplateName).execute()
      if (template != null) {
        println "Instance Template Name: ${template.getName()}"
        println "Instance Template Self Link: ${template.getSelfLink()}"

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

        def instanceMetadata = templateOpMap?.instanceMetadata
        if (instanceMetadata && autoscaler) {
          instanceMetadata.(GCEUtil.AUTOSCALING_POLICY) = objectMapper.writeValueAsString(autoscaler)
        } else if (autoscaler) {
          templateOpMap.instanceMetadata = [
            (GCEUtil.AUTOSCALING_POLICY): objectMapper.writeValueAsString(autoscaler)
          ]
        }

        if (templateOpMap.instanceMetadata) {
          def converter = atomicOperationsRegistry.getAtomicOperationConverter('modifyGoogleServerGroupInstanceTemplateDescription', 'gce')
          AtomicOperation templateOp = converter.convertOperation(templateOpMap)
          orchestrationProcessor.process('gce', [templateOp], UUID.randomUUID().toString())
        }
      } else {
        println "Instance Template not found in the specified project."
      }
    }else {
      println "Instance Template Link not found in the specified project."
    }
  }
*/


  void updatePolicyMetadata(Compute compute,
                            GoogleNamedAccountCredentials credentials,
                            String project,
                            String groupUrl,
                            autoscaler) {
    def groupName = Utils.getLocalName(groupUrl)
    def groupRegion = Utils.getRegionFromGroupUrl(groupUrl)

    String templateUrl = null
    switch (Utils.determineServerGroupType(groupUrl)) {
      case GoogleServerGroup.ServerGroupType.REGIONAL:
         Compute.RegionAutoscalers.Get getOperation = compute.regionAutoscalers().get(project, groupRegion, groupName)
        if (getOperation != null) {
          def  regionAutoscalar = timeExecute(
            getOperation,
          "compute.regionAutoscalers.get",
          TAG_SCOPE, SCOPE_REGIONAL, TAG_REGION, groupRegion)
          if(regionAutoscalar != null) {
            String instanceTemplateSelfLink = regionAutoscalar.getTarget()

            InstanceTemplate instanceTemplate = compute.instanceTemplates().get(project, instanceTemplateSelfLink).execute()
            if(instanceTemplate != null){
              templateUrl = instanceTemplateSelfLink
            }else {
              println( "instanceTemplateSelfLink ${instanceTemplateSelfLink} in regionAutoscalar.getTarget() " +
                "returned null in the specified zone and project.")
            }
          }else {
            println( "timeExecute for compute.regionAutoscalers().get(${project}, ${groupRegion}, ${groupName}) " +
              "returned null in the specified zone and project.")
          }
        }else {
          println( "compute.regionAutoscalers().get(${project}, ${groupRegion}, ${groupName}) " +
            "returned null in the specified zone and project.")
        }
        break
      case GoogleServerGroup.ServerGroupType.ZONAL:
        def groupZone = Utils.getZoneFromGroupUrl(groupUrl)
        Compute.Autoscalers.Get getOperation = compute.autoscalers().get(project, groupZone, groupName)
        if (getOperation != null) {
          def  autoscalar =timeExecute(
            getOperation,
            "compute.autoscalers.get",
            TAG_SCOPE, SCOPE_ZONAL, TAG_ZONE, groupZone)
          if(autoscalar != null) {
            String instanceTemplateSelfLink = autoscalar.getTarget()

            InstanceTemplate instanceTemplate = compute.instanceTemplates().get(project, instanceTemplateSelfLink).execute()
            if(instanceTemplate != null){
              templateUrl = instanceTemplateSelfLink
            }else {
              println( "instanceTemplateSelfLink ${instanceTemplateSelfLink} in autoscalar.getTarget() " +
                "returned null in the specified zone and project.")
            }
          }else {
            println( "timeExecute for compute.autoscalers().get(${project}, ${groupZone}, ${groupName}) " +
              "returned null in the specified zone and project.")
          }
        }else {
          println( "compute.autoscalers().get(${project}, ${groupZone}, ${groupName}) " +
            "returned null in the specified zone and project.")
        }

        break
      default:
        throw new IllegalStateException("Server group referenced by ${groupUrl} has illegal type.")
        break
    }
    if (templateUrl != null) {
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

      def instanceMetadata = templateOpMap?.instanceMetadata
      if (instanceMetadata && autoscaler) {
        instanceMetadata.(GCEUtil.AUTOSCALING_POLICY) = objectMapper.writeValueAsString(autoscaler)
      } else if (autoscaler) {
        templateOpMap.instanceMetadata = [
          (GCEUtil.AUTOSCALING_POLICY): objectMapper.writeValueAsString(autoscaler)
        ]
      }

      if (templateOpMap.instanceMetadata) {
        def converter = atomicOperationsRegistry.getAtomicOperationConverter('modifyGoogleServerGroupInstanceTemplateDescription', 'gce')
        AtomicOperation templateOp = converter.convertOperation(templateOpMap)
        orchestrationProcessor.process('gce', [templateOp], UUID.randomUUID().toString())
      }
    }else {
      println( " templateUrl ${templateUrl} not found in the specified region and project.")
    }
  }
}
