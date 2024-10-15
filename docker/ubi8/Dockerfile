FROM  quay.io/opsmxpublic/ubifips:8.7
MAINTAINER sig-platform@spinnaker.io
LABEL name='clouddriver'
LABEL maintainer='info@opsmx.io'
LABEL summary='Red Hat certified Open Enterprise Spinnaker ubi8 container image for clouddriver'
LABEL description='Certified Open Enterprise Spinnaker is an Enterprise grade, Red Hat certified and OpsMx supported release of the popular and critically acclaimed Continuous Delivery platform Spinnaker'
LABEL vendor='OpsMx'

RUN yum install -y python38
ARG TARGETARCH


ENV KUBECTL_RELEASE=1.31.0
ENV AWS_CLI_VERSION=1.18.152
ENV AWS_CLI_S3_CMD=2.0.2
ENV AWS_AIM_AUTHENTICATOR_VERSION=0.6.14
ENV GOOGLE_CLOUD_SDK_VERSION=496.0.0
ENV ECR_TOKEN_VERSION=v1.0.2

ENV PATH "$PATH:/usr/local/bin/:/opt/google-cloud-sdk/bin/:/usr/local/bin/aws-iam-authenticator"

USER root
RUN yum -y install bash jq  tar unzip wget unzip procps  java-17-openjdk-devel.x86_64 vim  net-tools curl git

# AWS CLI
RUN yum -y install python3-pip && \
    pip3 install --upgrade awscli==${AWS_CLI_VERSION} s3cmd==${AWS_CLI_S3_CMD} python-magic \
     && yum -y remove  python3-pip && \
    yum clean all


RUN   wget -nv https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${GOOGLE_CLOUD_SDK_VERSION}-linux-x86_64.tar.gz \
  && mkdir -p /opt \
  && tar -xzf google-cloud-sdk-${GOOGLE_CLOUD_SDK_VERSION}-linux-x86_64.tar.gz -C /opt \
  && rm google-cloud-sdk-${GOOGLE_CLOUD_SDK_VERSION}-linux-x86_64.tar.gz \
  && CLOUDSDK_PYTHON="python3" /opt/google-cloud-sdk/install.sh --usage-reporting=false --bash-completion=false \
     --additional-components app-engine-java app-engine-go gke-gcloud-auth-plugin \
  && rm -rf ~/.config/gcloud \
  && rm -rf /opt/google-cloud-sdk/.install/.backup

# kubectl + AWS IAM authenticator
RUN wget https://storage.googleapis.com/kubernetes-release/release/v${KUBECTL_RELEASE}/bin/linux/${TARGETARCH}/kubectl \
  && chmod +x kubectl \
  && mv ./kubectl /usr/local/bin/kubectl \
  && wget -O aws-iam-authenticator https://github.com/kubernetes-sigs/aws-iam-authenticator/releases/download/v${AWS_AIM_AUTHENTICATOR_VERSION}/aws-iam-authenticator_${AWS_AIM_AUTHENTICATOR_VERSION}_linux_${TARGETARCH} \
	&& chmod +x ./aws-iam-authenticator \
	&& mv ./aws-iam-authenticator /usr/local/bin/aws-iam-authenticator\
    && ln -sf /usr/local/bin/aws-iam-authenticator /usr/local/bin/heptio-authenticator-aws

COPY clouddriver-web/build/install/clouddriver /opt/clouddriver

RUN adduser spinnaker
RUN mkdir -p /opt/clouddriver/plugins
RUN chown -R spinnaker:spinnaker /opt/
RUN mkdir -p /etc/pki/tls/certs
RUN chmod -R 777 /etc/pki/tls/certs
RUN chmod -R 777 /var/
RUN chown -R spinnaker:spinnaker /var
USER spinnaker
HEALTHCHECK CMD curl --fail http://localhost:7002/health
CMD  sleep 180s ; "/opt/clouddriver/bin/clouddriver"
