---
subcategory: "Cloud Kafka(ckafka)"
layout: "tencentcloudenterprise"
page_title: "TencentCloudEnterprise: tencentcloudenterprise_ckafka_instance"
sidebar_current: "docs-tencentcloudenterprise-resource-ckafka_instance"
description: |-
  Use this resource to create ckafka instance.
---

# tencentcloudenterprise_ckafka_instance

Use this resource to create ckafka instance.

~> **NOTE:** Instance creation uses `HourCreate` with postpaid billing (`PayMode=0`).

Professional postpaid instances (`pid = 1004667`) support in-place increases to `band_width` and `disk_size` using `HourModify`. Standard instances retain replacement behavior for these fields. In-place decreases are rejected. A professional upgrade queries the current instance and the old and target `DescribeOrderConfig` configurations; both billing configurations are augmented with the target resource specifications, matching the console request format. `GoodsCategoryId` is omitted because its requiredness and source have not been established for this deployment.

The instance must be running, and `disk_type` and current topic/partition quotas must be available. When changing capacity, existing quotas are preserved unless a positive `topic` or `partition` value is configured; quota decreases are rejected. Topic/partition-only updates are not added by this change. Upgrade completion requires the instance to be running with the requested bandwidth, disk size and quotas; `timeouts.update` defaults to 30 minutes. An accepted `HourModify` response may contain `Result: {}`. Failed or ambiguous trade requests are not automatically resubmitted.

To select a Kubernetes hosting cluster for a professional instance, set `k8s_cluster_id = "tcs-cluster"`. The value is sent as `GoodsDetail.k8sClusterId`. It is distinct from the integer `cluster_id`; changing it replaces the instance. The name and architecture of the hosting cluster are not added as configurable parameters.

## Example Usage

### Basic Instance

```hcl
variable "vpc_id" {
  default = "vpc-68vi2d3h"
}

variable "subnet_id" {
  default = "subnet-ob6clqwk"
}

variable "product_info_list_standard" {
  type = list(object({
    name  = string
    value = string
  }))
  default = [
    {
      name  = "Region"
      value = "shenzhen"
    },
    {
      name  = "Cluster"
      value = "ap-shenzhen-region-jcctest-ops-1 cluster"
    },
    {
      name  = "AZ"
      value = "ap-shenzhen-region-jcctest-ops-1 & ap-shenzhen-region-jcctest-ops-2"
    },
    {
      name  = "Instance Name"
      value = "Not named"
    },
    {
      name  = "Specs Type"
      value = "Standard Edition"
    },
    {
      name  = "Product Model"
      value = "Basic"
    },
    {
      name  = "Peak Bandwidth"
      value = "40MB/s"
    },
    {
      name  = "Disk Capacity"
      value = "300GB"
    },
    {
      name  = "Message Retention Period"
      value = "24 hours"
    },
    {
      name  = "Network"
      value = "vpc-hqa3zhut"
    },
    {
      name  = "Subnet"
      value = "subnet-dot6weka",
    }
  ]
}

variable "product_info_list_profession" {
  type = list(object({
    name  = string
    value = string
  }))
  default = [
    {
      name  = "Region"
      value = "shenzhen"
    },
    {
      name  = "Multi-AZ"
      value = "ap-shenzhen-region-jcctest-ops-1 & ap-shenzhen-region-jcctest-ops-2"
    },
    {
      name  = "Instance Name"
      value = "Not named"
    },
    {
      name  = "Specs Type"
      value = "Pro Edition"
    },
    {
      name  = "Kafka Version"
      value = "2.4.1"
    },
    {
      name  = "Peak Bandwidth"
      value = "1600MB/s"
    },
    {
      name  = "Disk Capacity"
      value = "10000GB"
    },
    {
      name  = "Topic"
      value = 2000
    },
    {
      name  = "Partition"
      value = 4000
    },
    {
      name  = "Message Retention Period"
      value = "24 hours"
    },
    {
      name  = "Network"
      value = "vpc-hqa3zhut"
    },
    {
      name  = "Subnet"
      value = "subnet-dot6weka"
    }
  ]
}

data "tencentcloudenterprise_availability_zones" "gz" {
  name = "ap-beijing-region-jcctest-ops-1"
}

resource "tencentcloudenterprise_ckafka_instance" "kafka_instance" {
  instance_name      = "ckafka-instance-type-tf-test"
  zone_id            = data.tencentcloudenterprise_availability_zones.gz.zones.0.id
  region_id          = 80000052
  region_name        = "深圳"
  pid                = 1004667
  vpc_id             = var.vpc_id
  subnet_id          = var.subnet_id
  msg_retention_time = 1300
  renew_flag         = 0
  kafka_version      = "2.4.1"
  band_width         = 1024
  disk_size          = 1000
  disk_type          = "SSD"
  instance_type      = "Basic"
  topic              = 20
  partition          = 4
  goods_num          = 1
  dynamic "product_info" {
    for_each = var.product_info_list_profession
    content {
      name  = product_info.value["name"]
      value = product_info.value["value"]
    }
  }
}
```

### Multi zone Instance

```hcl
variable "vpc_id" {
  default = "vpc-68vi2d3h"
}

variable "subnet_id" {
  default = "subnet-ob6clqwk"
}

data "tencentcloudenterprise_availability_zones" "gz3" {
  name    = "ap-guangzhou-3"
  product = "ckafka"
}

data "tencentcloudenterprise_availability_zones" "gz6" {
  name    = "ap-guangzhou-6"
  product = "ckafka"
}

resource "tencentcloudenterprise_ckafka_instance" "kafka_instance" {
  instance_name   = "ckafka-instance-maz-tf-test"
  zone_id         = data.tencentcloudenterprise_availability_zones.gz3.zones.0.id
  multi_zone_flag = true
  zone_ids = [
    data.tencentcloudenterprise_availability_zones.gz3.zones.0.id,
    data.tencentcloudenterprise_availability_zones.gz6.zones.0.id
  ]
  period             = 1
  vpc_id             = var.vpc_id
  subnet_id          = var.subnet_id
  msg_retention_time = 1300
  renew_flag         = 0
  kafka_version      = "1.1.1"
  disk_size          = 500
  disk_type          = "CLOUD_BASIC"
  region_id          = 80000052
  region_name        = "深圳"
  pid                = 1
}
```

## Argument Reference

The following arguments are supported:

* `band_width` - (Required, Int) Instance bandwidth in MB/s. Professional postpaid instances support in-place increases. Changes replace standard instances.
* `disk_size` - (Required, Int) Disk capacity in GB. Professional postpaid instances support in-place increases. Changes replace standard instances. The target must conform to the platform's bandwidth and storage specification rules.
* `goods_num` - (Required, Int) Quantity.
* `instance_name` - (Required, String) Instance name.
* `pid` - (Required, Int, ForceNew) Pricing formula ID. 1-9.
* `product_info` - (Required, List) Product information. When Specs Type is Standard Edition, Instance Name is optional. When Specs Type is Pro Edition, Instance Name and Product Model are optional; all other fields are required:
  - name: Region, value: example-region
  - name: Cluster, value: example-cluster
  - name: AZ, value: example-az
  - name: Instance Name, value: test111
  - name: Specs Type, value: Standard Edition
  - name: Product Model, value: Basic
  - name: Peak Bandwidth, value: 40MB/s
  - name: Disk Capacity, value: 300GB
  - name: Message Retention Period, value: 72 hours
  - name: Network, value: vpc-kltzarib
  - name: Subnet, value: subnet-7qt1q9h6.
* `region_id` - (Required, Int) Region ID.
* `region_name` - (Required, String) Region Name.
* `subnet_id` - (Required, String) Subnet id.
* `vpc_id` - (Required, String) Vpc id.
* `zone_id` - (Required, Int) Available zone id.
* `cluster_id` - (Optional, Int, ForceNew) Cluster-ID represents  the cluster to which Ckafa belongs, associated with cluster_name only needs to select one of them.
* `k8s_cluster_id` - (Optional, String, ForceNew) Kubernetes hosting cluster ID for a professional instance. Nonempty, non-whitespace string. Sent as `GoodsDetail.k8sClusterId` on creation.
* `disk_type` - (Optional, String) Type of disk. [SSD].
* `instance_id` - (Optional, String) Ckafka instance ID.
* `instance_type` - (Optional, String) Description of instance type. [Basic, High Performance].
* `kafka_version` - (Optional, String) Kafka version (2.4.1/2.8.1).
* `msg_retention_time` - (Optional, Int) The maximum retention time of instance logs, in minutes. the default is 10080 (7 days), the maximum is 30 days, and the default 0 is not filled, which means that the log retention time recovery policy is not enabled.
* `multi_zone_flag` - (Optional, Bool) Indicates whether the instance is multi zones. NOTE: if set to `true`, `zone_ids` must set together.
* `partition` - (Optional, Int) Partition Size. Its interval varies with bandwidth, and the input must be within the interval, which can be viewed through the control. If it is not within the interval, the plan will cause a change when first created.
* `renew_flag` - (Optional, Int) Prepaid automatic renewal mark, 0 means the default state, the initial state, 1 means automatic renewal, 2 means clear no automatic renewal (user setting).
* `tag_set` - (Optional, Map) Tag set of instance.
* `topic` - (Optional, Int) Topic Size.
* `zone_ids` - (Optional, Set: [`Int`]) List of available zone id. NOTE: this argument must set together with `multi_zone_flag`.

The `product_info` object supports the following:

* `name` - (Required, String) Item name.
* `value` - (Required, String) Item value.

## Attributes Reference

In addition to all arguments above, the following attributes are exported:

* `id` - ID of the resource.
* `vip` - Vip of instance.
* `vport` - Type of instance.

## Import

tencentcloudenterprise_ckafka_instance can be imported using the id, e.g.

```
ckafka instance can be imported using the instance_id, e.g.
```
$ terraform import tencentcloudenterprise_ckafka_instance.foo ckafka-f9ife4zz
```
```

