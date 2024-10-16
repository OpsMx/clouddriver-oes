/*
 * Copyright 2020 Google, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.spinnaker.clouddriver.kubernetes.artifact;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.netflix.spinnaker.clouddriver.artifacts.kubernetes.KubernetesArtifactType;
import com.netflix.spinnaker.clouddriver.kubernetes.artifact.ArtifactReplacer.ReplaceResult;
import com.netflix.spinnaker.clouddriver.kubernetes.description.manifest.KubernetesManifest;
import com.netflix.spinnaker.kork.artifacts.model.Artifact;
import io.kubernetes.client.openapi.JSON;
import io.kubernetes.client.openapi.models.V1ConfigMapEnvSource;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1CrossVersionObjectReference;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1DeploymentSpec;
import io.kubernetes.client.openapi.models.V1EnvFromSource;
import io.kubernetes.client.openapi.models.V1HorizontalPodAutoscaler;
import io.kubernetes.client.openapi.models.V1HorizontalPodAutoscalerSpec;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1ReplicaSet;
import io.kubernetes.client.openapi.models.V1ReplicaSetSpec;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

final class ArtifactReplacerTest {
  // We serialized generated Kubernetes metadata objects with JSON io.kubernetes.client.openapi.JSON
  // so that they match what we get back from kubectl.  We'll just gson from converting to a
  // KubernetesManifest because that's what we currently use to parse the result from kubectl and
  // we want this test to be realistic.
  private static final JSON json = new JSON();
  private static final Gson gson = new Gson();

  private static final String NAMESPACE = "ns";
  private static final String ACCOUNT = "my-account";
  private static final String DEFAULT_BINDING = "match-name-and-tag";

  @Test
  void extractsDeploymentNameFromHpa() {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.hpaDeployment()));
    KubernetesManifest hpa = getHpa("Deployment", "my-deployment");
    Set<Artifact> artifacts = artifactReplacer.findAll(hpa);

    assertThat(artifacts).hasSize(1);
    Artifact artifact = Iterables.getOnlyElement(artifacts);
    assertThat(artifact.getName()).isEqualTo("my-deployment");
    assertThat(artifact.getType()).isEqualTo(KubernetesArtifactType.Deployment.getType());
  }

  @Test
  void skipsHpaWithUnknownKind() {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.hpaDeployment()));
    KubernetesManifest hpa = getHpa("Unknown", "my-deployment");
    Set<Artifact> artifacts = artifactReplacer.findAll(hpa);

    assertThat(artifacts).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("imageArtifactTestCases")
  void extractsDockerImageArtifacts(ImageTestCase testCase) {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.dockerImage()));
    KubernetesManifest deployment = getDeploymentWithContainer(getContainer(testCase.getImage()));
    Set<Artifact> artifacts = artifactReplacer.findAll(deployment);

    assertThat(artifacts).hasSize(1);
    Artifact artifact = Iterables.getOnlyElement(artifacts);
    assertThat(artifact.getType()).isEqualTo(KubernetesArtifactType.DockerImage.getType());
    assertThat(artifact.getName()).isEqualTo(testCase.getName());
    assertThat(artifact.getReference()).isEqualTo(testCase.getImage());
  }

  @ParameterizedTest
  @MethodSource("imageArtifactTestCases")
  void extractsDockerImageArtifactsFromInitContainers(ImageTestCase testCase) {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.dockerImage()));
    KubernetesManifest deployment =
        getDeploymentWithInitContainer(getContainer(testCase.getImage()));
    Set<Artifact> artifacts = artifactReplacer.findAll(deployment);

    assertThat(artifacts).hasSize(1);
    Artifact artifact = Iterables.getOnlyElement(artifacts);
    assertThat(artifact.getType()).isEqualTo(KubernetesArtifactType.DockerImage.getType());
    assertThat(artifact.getName()).isEqualTo(testCase.getName());
    assertThat(artifact.getReference()).isEqualTo(testCase.getImage());
  }

  // Called by @MethodSource which error-prone does not detect.
  @SuppressWarnings("unused")
  private static Stream<ImageTestCase> imageArtifactTestCases() {
    return Stream.of(
        ImageTestCase.of("nginx:112", "nginx"),
        ImageTestCase.of("nginx:1.12-alpine", "nginx"),
        ImageTestCase.of("my-nginx:100000", "my-nginx"),
        ImageTestCase.of("my.nginx:100000", "my.nginx"),
        ImageTestCase.of("reg/repo:1.2.3", "reg/repo"),
        ImageTestCase.of("reg.repo:123@sha256:13", "reg.repo:123"),
        ImageTestCase.of("reg.default.svc/r/j:485fabc", "reg.default.svc/r/j"),
        ImageTestCase.of("reg:5000/r/j:485fabc", "reg:5000/r/j"),
        ImageTestCase.of("reg:5000/r__j:485fabc", "reg:5000/r__j"),
        ImageTestCase.of("clouddriver", "clouddriver"),
        ImageTestCase.of("clouddriver@sha256:9145", "clouddriver"),
        ImageTestCase.of(
            "localhost:5000/test/busybox@sha256:cbbf22", "localhost:5000/test/busybox"));
  }

  @RequiredArgsConstructor
  @Value
  private static class ImageTestCase {
    final String image;
    final String name;

    static ImageTestCase of(String image, String name) {
      return new ImageTestCase(image, name);
    }
  }

  @Test
  void emptyReplace() {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.dockerImage()));
    KubernetesManifest deployment = getDeploymentWithContainer(getContainer("nginx:112"));

    ReplaceResult result =
        artifactReplacer.replaceAll(
            DEFAULT_BINDING, deployment, ImmutableList.of(), NAMESPACE, ACCOUNT);

    assertThat(result.getManifest()).isEqualTo(deployment);
    assertThat(result.getBoundArtifacts()).isEmpty();
  }

  @Test
  void replacesDockerImage() {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.dockerImage()));
    KubernetesManifest deployment = getDeploymentWithContainer(getContainer("nginx"));

    Artifact inputArtifact =
        Artifact.builder().type("docker/image").name("nginx").reference("nginx:1.19.1").build();
    ReplaceResult result =
        artifactReplacer.replaceAll(
            DEFAULT_BINDING, deployment, ImmutableList.of(inputArtifact), NAMESPACE, ACCOUNT);

    assertThat(extractImage(result.getManifest())).contains("nginx:1.19.1");
    assertThat(result.getBoundArtifacts()).hasSize(1);
    assertThat(Iterables.getOnlyElement(result.getBoundArtifacts())).isEqualTo(inputArtifact);
  }

  @Test
  void replacesDockerImageWithTag() {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.dockerImage()));
    KubernetesManifest deployment = getDeploymentWithContainer(getContainer("nginx:1.18.0"));

    Artifact inputArtifact =
        Artifact.builder().type("docker/image").name("nginx").reference("nginx:1.19.1").build();
    ReplaceResult result =
        artifactReplacer.replaceAll(
            DEFAULT_BINDING, deployment, ImmutableList.of(inputArtifact), NAMESPACE, ACCOUNT);

    assertThat(extractImage(result.getManifest())).contains("nginx:1.19.1");
    assertThat(result.getBoundArtifacts()).hasSize(1);
    assertThat(Iterables.getOnlyElement(result.getBoundArtifacts())).isEqualTo(inputArtifact);
  }

  /**
   * This is a support for a legacy behavior, it's disabled by default and enabled by
   * kubernetes.artifact-binding.docker-image with value 'match-name-only'. If there is already a
   * tag on the image in the manifest, we are not replacing it.
   */
  @Test
  void doesNotReplaceImageWithTag() {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.dockerImage()));
    KubernetesManifest deployment = getDeploymentWithContainer(getContainer("nginx:1.18.0"));

    Artifact inputArtifact =
        Artifact.builder().type("docker/image").name("nginx").reference("nginx:1.19.1").build();
    ReplaceResult result =
        artifactReplacer.replaceAll(
            "match-name-only", deployment, ImmutableList.of(inputArtifact), NAMESPACE, ACCOUNT);

    assertThat(result.getManifest()).isEqualTo(deployment);
    assertThat(result.getBoundArtifacts()).isEmpty();
  }

  /**
   * Only artifacts of type kubernetes/* need to have the same account as the manifest to be
   * replaced.
   */
  @Test
  void nonKubernetesArtifactIgnoresDifferentAccount() {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.dockerImage()));
    KubernetesManifest deployment = getDeploymentWithContainer(getContainer("nginx"));

    Artifact inputArtifact =
        Artifact.builder()
            .type("docker/image")
            .name("nginx")
            .putMetadata("account", "another-account")
            .reference("nginx:1.19.1")
            .build();
    ReplaceResult result =
        artifactReplacer.replaceAll(
            DEFAULT_BINDING, deployment, ImmutableList.of(inputArtifact), NAMESPACE, ACCOUNT);

    assertThat(extractImage(result.getManifest())).contains("nginx:1.19.1");
    assertThat(result.getBoundArtifacts()).hasSize(1);
    assertThat(Iterables.getOnlyElement(result.getBoundArtifacts())).isEqualTo(inputArtifact);
  }

  /**
   * Only artifacts of type kubernetes/* need to have the same namespace as the manifest to be
   * replaced.
   */
  @Test
  void nonKubernetesArtifactIgnoresDifferentNamespace() {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.dockerImage()));
    KubernetesManifest deployment = getDeploymentWithContainer(getContainer("nginx"));

    Artifact inputArtifact =
        Artifact.builder()
            .type("docker/image")
            .name("nginx")
            .location("another-namespace")
            .reference("nginx:1.19.1")
            .build();
    ReplaceResult result =
        artifactReplacer.replaceAll(
            DEFAULT_BINDING, deployment, ImmutableList.of(inputArtifact), NAMESPACE, ACCOUNT);

    assertThat(extractImage(result.getManifest())).contains("nginx:1.19.1");
    assertThat(result.getBoundArtifacts()).hasSize(1);
    assertThat(Iterables.getOnlyElement(result.getBoundArtifacts())).isEqualTo(inputArtifact);
  }

  @Test
  void replacesConfigMap() {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.configMapEnv()));
    KubernetesManifest replicaSet = getReplicaSetWithEnvFrom("my-config-map");

    Artifact inputArtifact =
        Artifact.builder()
            .type("kubernetes/configMap")
            .name("my-config-map")
            .location(NAMESPACE)
            .version("v003")
            .reference("my-config-map-v003")
            .putMetadata("account", ACCOUNT)
            .build();
    ReplaceResult result =
        artifactReplacer.replaceAll(
            DEFAULT_BINDING, replicaSet, ImmutableList.of(inputArtifact), NAMESPACE, ACCOUNT);

    assertThat(extractEnvRef(result.getManifest())).contains("my-config-map-v003");
    assertThat(result.getBoundArtifacts()).hasSize(1);

    Artifact replacedArtifact = Iterables.getOnlyElement(result.getBoundArtifacts());
    assertThat(replacedArtifact).isEqualTo(inputArtifact);
  }

  @Test
  void replacesConfigMapArtifactMissingAccount() {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.configMapEnv()));
    KubernetesManifest replicaSet = getReplicaSetWithEnvFrom("my-config-map");

    Artifact inputArtifact =
        Artifact.builder()
            .type("kubernetes/configMap")
            .name("my-config-map")
            .location(NAMESPACE)
            .version("v003")
            .reference("my-config-map-v003")
            .build();
    ReplaceResult result =
        artifactReplacer.replaceAll(
            DEFAULT_BINDING, replicaSet, ImmutableList.of(inputArtifact), NAMESPACE, ACCOUNT);

    assertThat(extractEnvRef(result.getManifest())).contains("my-config-map-v003");
    assertThat(result.getBoundArtifacts()).hasSize(1);

    Artifact replacedArtifact = Iterables.getOnlyElement(result.getBoundArtifacts());
    assertThat(replacedArtifact).isEqualTo(inputArtifact);
  }

  @Test
  void doesNotReplaceConfigmapWrongAccount() {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.configMapEnv()));
    KubernetesManifest replicaSet = getReplicaSetWithEnvFrom("my-config-map");

    Artifact inputArtifact =
        Artifact.builder()
            .type("kubernetes/configMap")
            .name("my-config-map")
            .location(NAMESPACE)
            .version("v003")
            .reference("my-config-map-v003")
            .putMetadata("account", "other-account")
            .build();
    ReplaceResult result =
        artifactReplacer.replaceAll(
            DEFAULT_BINDING, replicaSet, ImmutableList.of(inputArtifact), NAMESPACE, ACCOUNT);

    assertThat(extractEnvRef(result.getManifest())).contains("my-config-map");
    assertThat(result.getBoundArtifacts()).hasSize(0);
  }

  @Test
  void doesNotReplaceConfigmapWrongNamespace() {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.configMapEnv()));
    KubernetesManifest replicaSet = getReplicaSetWithEnvFrom("my-config-map");

    Artifact inputArtifact =
        Artifact.builder()
            .type("kubernetes/configMap")
            .name("my-config-map")
            .location("other-namespace")
            .version("v003")
            .reference("my-config-map-v003")
            .putMetadata("account", ACCOUNT)
            .build();
    ReplaceResult result =
        artifactReplacer.replaceAll(
            DEFAULT_BINDING, replicaSet, ImmutableList.of(inputArtifact), NAMESPACE, ACCOUNT);

    assertThat(extractEnvRef(result.getManifest())).contains("my-config-map");
    assertThat(result.getBoundArtifacts()).hasSize(0);
  }

  @Test
  void replacesConfigMapNoNamespace() {
    ArtifactReplacer artifactReplacer =
        new ArtifactReplacer(ImmutableList.of(Replacer.configMapEnv()));
    KubernetesManifest replicaSet = getReplicaSetWithEnvFrom("my-config-map");

    Artifact inputArtifact =
        Artifact.builder()
            .type("kubernetes/configMap")
            .name("my-config-map")
            .version("v003")
            .reference("my-config-map-v003")
            .putMetadata("account", ACCOUNT)
            .build();
    ReplaceResult result =
        artifactReplacer.replaceAll(
            DEFAULT_BINDING, replicaSet, ImmutableList.of(inputArtifact), "", ACCOUNT);

    assertThat(extractEnvRef(result.getManifest())).contains("my-config-map-v003");
    assertThat(result.getBoundArtifacts()).hasSize(1);

    Artifact replacedArtifact = Iterables.getOnlyElement(result.getBoundArtifacts());
    assertThat(replacedArtifact).isEqualTo(inputArtifact);
  }

  // Extracts the first container image from a Kubernetes manifest representing a deployment
  private Optional<String> extractImage(KubernetesManifest manifest) {
    // We want to use the Kubernetes-supported json deserializer so need to first serialize
    // the manifest to a string.
    V1Deployment deployment = json.deserialize(json.serialize(manifest), V1Deployment.class);
    return Optional.ofNullable(deployment.getSpec())
        .map(V1DeploymentSpec::getTemplate)
        .map(V1PodTemplateSpec::getSpec)
        .map(V1PodSpec::getContainers)
        .map(c -> c.get(0))
        .map(V1Container::getImage);
  }

  // Extracts the config map ref for the first env ref for the first container in a Kubernetes
  // manifest representing a deployment.
  private Optional<String> extractEnvRef(KubernetesManifest manifest) {
    // We want to use the Kubernetes-supported json deserializer so need to first serialize
    // the manifest to a string.
    V1ReplicaSet replicaSet = json.deserialize(json.serialize(manifest), V1ReplicaSet.class);
    return Optional.ofNullable(replicaSet.getSpec())
        .map(V1ReplicaSetSpec::getTemplate)
        .map(V1PodTemplateSpec::getSpec)
        .map(V1PodSpec::getContainers)
        .map(c -> c.get(0))
        .map(V1Container::getEnvFrom)
        .map(e -> e.get(0))
        .map(V1EnvFromSource::getConfigMapRef)
        .map(V1ConfigMapEnvSource::getName);
  }

  private KubernetesManifest getHpa(String kind, String name) {
    String hpa =
        json.serialize(
            new V1HorizontalPodAutoscaler()
                .metadata(new V1ObjectMeta().name("my-hpa").namespace("default"))
                .spec(
                    new V1HorizontalPodAutoscalerSpec()
                        .scaleTargetRef(
                            new V1CrossVersionObjectReference()
                                .apiVersion("apps/v1")
                                .kind(kind)
                                .name(name))));
    return gson.fromJson(hpa, KubernetesManifest.class);
  }

  private V1Container getContainer(String image) {
    V1Container v1Container = new V1Container();
    v1Container.image(image);
    v1Container.name("container");
    v1Container.addPortsItem(new V1ContainerPort().containerPort(80));

    return v1Container;
  }

  private KubernetesManifest getDeploymentWithContainer(V1Container container) {
    return getDeployment(ImmutableList.of(container), ImmutableList.of());
  }

  private KubernetesManifest getDeploymentWithInitContainer(V1Container container) {
    return getDeployment(ImmutableList.of(), ImmutableList.of(container));
  }

  private KubernetesManifest getDeployment(
      Collection<V1Container> containers, Collection<V1Container> initContainers) {
    String deployment =
        json.serialize(
            new V1Deployment()
                .metadata(
                    new V1ObjectMeta()
                        .name("my-app-deployment")
                        .labels(ImmutableMap.of("app", "my-app")))
                .spec(
                    new V1DeploymentSpec()
                        .replicas(3)
                        .selector(
                            new V1LabelSelector().matchLabels(ImmutableMap.of("app", "my-app")))
                        .template(
                            new V1PodTemplateSpec()
                                .metadata(
                                    new V1ObjectMeta().labels(ImmutableMap.of("app", "my-app")))
                                .spec(
                                    new V1PodSpec()
                                        .containers(containers.stream().toList())
                                        .initContainers(initContainers.stream().toList())))));
    return gson.fromJson(deployment, KubernetesManifest.class);
  }

  private KubernetesManifest getReplicaSetWithEnvFrom(String configMapRef) {
    String deployment =
        json.serialize(
            new V1ReplicaSet()
                .metadata(new V1ObjectMeta().name("my-app-deployment"))
                .spec(
                    new V1ReplicaSetSpec()
                        .replicas(3)
                        .template(
                            new V1PodTemplateSpec()
                                .spec(
                                    new V1PodSpec()
                                        .addContainersItem(
                                            new V1Container()
                                                .addEnvFromItem(
                                                    new V1EnvFromSource()
                                                        .configMapRef(
                                                            new V1ConfigMapEnvSource()
                                                                .name(configMapRef))))))));
    return gson.fromJson(deployment, KubernetesManifest.class);
  }
}
