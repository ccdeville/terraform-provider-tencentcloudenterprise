package tencentcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	ckafka "terraform-provider-tencentcloudenterprise/sdk/ckafka/v20190819"
	"terraform-provider-tencentcloudenterprise/tencentcloud/internal/helper"
	"terraform-provider-tencentcloudenterprise/tencentcloud/ratelimit"
)

func customizeCkafkaInstanceCapacityDiff(_ context.Context, d *schema.ResourceDiff, _ interface{}) error {
	if d.NewValueKnown("pid") && d.Get("pid").(int) != CkafkaProfessionPid && d.Get("k8s_cluster_id").(string) != "" {
		return fmt.Errorf("k8s_cluster_id is only supported for professional Kafka instances (pid=%d)", CkafkaProfessionPid)
	}
	if d.Id() == "" || !d.NewValueKnown("pid") {
		return nil
	}
	for _, key := range []string{"band_width", "disk_size"} {
		if !d.HasChange(key) {
			continue
		}
		if d.Get("pid").(int) != CkafkaProfessionPid {
			if err := d.ForceNew(key); err != nil {
				return err
			}
			continue
		}
		// A new instance may have smaller specifications than the replaced one.
		if d.HasChange("pid") || d.HasChange("cluster_id") || d.HasChange("k8s_cluster_id") || !d.NewValueKnown(key) {
			continue
		}
		old, target := d.GetChange(key)
		if target.(int) <= 0 || target.(int) < old.(int) {
			return fmt.Errorf("%s must be positive and cannot decrease during an in-place professional Kafka upgrade", key)
		}
	}
	return nil
}

type ckafkaCapacity struct {
	Bandwidth int64 // MB/s, matching the HCL field and DescribeOrderConfig.
	DiskSize  int64 // GB.
	Topic     int64
	Partition int64
}

func ckafkaQuotaTotal(created, remaining int64) (int64, error) {
	if created < 0 || remaining < 0 || created > math.MaxInt64-remaining {
		return 0, fmt.Errorf("invalid Kafka instance quota counts")
	}
	return created + remaining, nil
}

func setCkafkaInstanceQuotas(d *schema.ResourceData, info *ckafka.InstanceAttributesResponse) error {
	for key, values := range map[string][2]*int64{
		"topic":     {info.CreatedTopics, info.RemainderTopics},
		"partition": {info.CreatedPartitions, info.RemainderPartitions},
	} {
		if values[0] == nil || values[1] == nil {
			continue
		}
		total, err := ckafkaQuotaTotal(*values[0], *values[1])
		if err != nil {
			return err
		}
		if err := d.Set(key, total); err != nil {
			return err
		}
	}
	return nil
}

func ckafkaCapacityFromAttributes(info *ckafka.InstanceAttributesResponse) (ckafkaCapacity, error) {
	if info == nil || info.Bandwidth == nil || info.DiskSize == nil || *info.Bandwidth <= 0 || *info.Bandwidth%8 != 0 || *info.DiskSize <= 0 {
		return ckafkaCapacity{}, fmt.Errorf("Kafka instance query did not return valid bandwidth (Mbps) and disk size")
	}
	if info.CreatedTopics == nil || info.RemainderTopics == nil || info.CreatedPartitions == nil || info.RemainderPartitions == nil {
		return ckafkaCapacity{}, fmt.Errorf("Kafka instance query did not return topic and partition quotas required to preserve the current configuration")
	}
	topic, err := ckafkaQuotaTotal(*info.CreatedTopics, *info.RemainderTopics)
	if err != nil {
		return ckafkaCapacity{}, err
	}
	partition, err := ckafkaQuotaTotal(*info.CreatedPartitions, *info.RemainderPartitions)
	if err != nil {
		return ckafkaCapacity{}, err
	}
	return ckafkaCapacity{Bandwidth: *info.Bandwidth / 8, DiskSize: *info.DiskSize, Topic: topic, Partition: partition}, nil
}

func (me *CkafkaService) describeCkafkaUpgradeAttributes(ctx context.Context, id string) (*ckafka.InstanceAttributesResponse, error) {
	var info *ckafka.InstanceAttributesResponse
	var resultMu sync.Mutex
	err := resource.RetryContext(ctx, readRetryTimeout, func() *resource.RetryError {
		if err := ctx.Err(); err != nil {
			return resource.NonRetryableError(err)
		}
		req := ckafka.NewDescribeInstanceAttributesRequest()
		req.InstanceId = helper.String(id)
		ratelimit.Check(req.GetAction())
		resp, err := me.client.UseCkafkaClient().DescribeInstanceAttributes(req)
		if err != nil {
			return retryError(err)
		}
		if resp == nil || resp.Response == nil || resp.Response.Result == nil {
			return resource.NonRetryableError(fmt.Errorf("DescribeInstanceAttributes returned an empty result for %s", id))
		}
		resultMu.Lock()
		info = resp.Response.Result
		resultMu.Unlock()
		return nil
	})
	resultMu.Lock()
	defer resultMu.Unlock()
	return info, err
}

func (me *CkafkaService) describeCkafkaOrderConfig(ctx context.Context, capacity ckafkaCapacity, diskType string) (map[string]interface{}, error) {
	var config map[string]interface{}
	var resultMu sync.Mutex
	err := resource.RetryContext(ctx, readRetryTimeout, func() *resource.RetryError {
		if err := ctx.Err(); err != nil {
			return resource.NonRetryableError(err)
		}
		req := ckafka.NewDescribeOrderConfigRequest()
		req.InstanceType = helper.String("profession")
		req.Bandwidth = helper.Int64(capacity.Bandwidth)
		req.DiskSize = helper.Int64(capacity.DiskSize)
		req.DiskType = helper.String(diskType)
		req.Topic = helper.Int64(capacity.Topic)
		req.Partition = helper.Int64(capacity.Partition)
		req.InquireFlag = helper.Int64(-1)
		ratelimit.Check(req.GetAction())
		resp, err := me.client.UseCkafkaClient().DescribeOrderConfig(req)
		if err != nil {
			return retryError(err)
		}
		if resp == nil || resp.Response == nil || resp.Response.Result == nil || resp.Response.Result.OrderConfig == nil {
			return resource.NonRetryableError(fmt.Errorf("DescribeOrderConfig returned no OrderConfig"))
		}
		decoder := json.NewDecoder(strings.NewReader(*resp.Response.Result.OrderConfig))
		decoder.UseNumber()
		var decoded map[string]interface{}
		if err := decoder.Decode(&decoded); err != nil {
			return resource.NonRetryableError(fmt.Errorf("invalid OrderConfig JSON: %w", err))
		}
		if len(decoded) == 0 || decoded["productCode"] != CKafkaProductCode || decoded["subProductCode"] != CkafkaSubCode_Profession {
			return resource.NonRetryableError(fmt.Errorf("DescribeOrderConfig returned an empty or unexpected professional product configuration"))
		}
		pid, ok := decoded["pid"].(json.Number)
		if !ok || string(pid) != fmt.Sprint(CkafkaProfessionPid) {
			return resource.NonRetryableError(fmt.Errorf("DescribeOrderConfig returned an unexpected pid"))
		}
		resultMu.Lock()
		config = decoded
		resultMu.Unlock()
		return nil
	})
	resultMu.Lock()
	defer resultMu.Unlock()
	return config, err
}

func buildCkafkaHourModifyRequest(d *schema.ResourceData, oldConfig, newConfig map[string]interface{}, target ckafkaCapacity) (*ckafka.HourModifyRequest, error) {
	if target.Bandwidth <= 0 || target.Bandwidth > math.MaxInt64/8 || target.DiskSize <= 0 || target.Topic < 0 || target.Partition < 0 {
		return nil, fmt.Errorf("invalid target Kafka capacity")
	}
	// Match the console: both billing configurations carry the target resource
	// specification; the old billing items themselves must remain unchanged.
	withTarget := func(config map[string]interface{}) map[string]interface{} {
		result := make(map[string]interface{}, len(config)+4)
		for key, value := range config {
			result[key] = value
		}
		result["bandWidth"] = target.Bandwidth * 8 // HourModify uses Mbps.
		result["diskSize"] = target.DiskSize
		result["topic"] = target.Topic
		result["partition"] = target.Partition
		return result
	}
	old, next := withTarget(oldConfig), withTarget(newConfig)
	next["vpcId"] = d.Get("vpc_id").(string)
	next["subnetId"] = d.Get("subnet_id").(string)
	detail, err := json.Marshal(map[string]interface{}{
		"resourceId": d.Id(), "productCode": CKafkaProductCode,
		"subProductCode": CkafkaSubCode_Profession,
		"oldConfig":      old, "newConfig": next, "productInfo": buildProductInfo(d),
	})
	if err != nil {
		return nil, err
	}
	req := ckafka.NewHourModifyRequest()
	req.InstanceId = helper.String(d.Id())
	req.GoodsInfo = &ckafka.GoodsInfo{
		RegionId: helper.IntInt64(d.Get("region_id").(int)),
		ZoneId:   helper.IntInt64(d.Get("zone_id").(int)),
		GoodsNum: helper.Int64(1), PayMode: helper.Int64(0),
		Type: helper.String(CKafkaProductCode), GoodsDetail: helper.String(string(detail)),
	}
	return req, nil
}

func updateCkafkaInstanceCapacity(ctx context.Context, d *schema.ResourceData, service *CkafkaService) error {
	if d.Get("pid").(int) != CkafkaProfessionPid {
		return fmt.Errorf("in-place capacity changes are only supported for professional Kafka instances")
	}
	ctx, cancel := context.WithTimeout(ctx, d.Timeout(schema.TimeoutUpdate))
	defer cancel()
	info, err := service.describeCkafkaUpgradeAttributes(ctx, d.Id())
	if err != nil {
		return fmt.Errorf("query Kafka instance before upgrade: %w", err)
	}
	if info.PayMode != nil && *info.PayMode != 0 {
		return fmt.Errorf("in-place capacity changes require a confirmed postpaid Kafka instance (PayMode=0)")
	}
	current, err := ckafkaCapacityFromAttributes(info)
	if err != nil {
		return err
	}
	target := current
	target.Bandwidth = int64(d.Get("band_width").(int))
	target.DiskSize = int64(d.Get("disk_size").(int))
	for key, value := range map[string]*int64{"topic": &target.Topic, "partition": &target.Partition} {
		if v, ok := d.GetOk(key); ok {
			*value = int64(v.(int))
		}
	}
	if target.Bandwidth <= 0 || target.Bandwidth > math.MaxInt64/8 || target.DiskSize <= 0 || target.Bandwidth < current.Bandwidth || target.DiskSize < current.DiskSize || target.Topic < current.Topic || target.Partition < current.Partition {
		return fmt.Errorf("Kafka capacity upgrade cannot decrease bandwidth, disk size, topic quota or partition quota")
	}
	if info.Status == nil || *info.Status != 1 {
		return fmt.Errorf("Kafka instance %s must be running before a capacity upgrade; wait for any previous operation to finish", d.Id())
	}
	if target == current {
		return nil // A previous request may have completed after an API timeout.
	}
	diskType := d.Get("disk_type").(string)
	if strings.TrimSpace(diskType) == "" {
		return fmt.Errorf("disk_type must be known to obtain the current and target Kafka billing configurations")
	}
	oldConfig, err := service.describeCkafkaOrderConfig(ctx, current, diskType)
	if err != nil {
		return fmt.Errorf("query old Kafka order configuration: %w", err)
	}
	newConfig, err := service.describeCkafkaOrderConfig(ctx, target, diskType)
	if err != nil {
		return fmt.Errorf("query target Kafka order configuration: %w", err)
	}
	req, err := buildCkafkaHourModifyRequest(d, oldConfig, newConfig, target)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ratelimit.Check(req.GetAction())
	// Do not retry a possibly accepted trade request: a later refresh and query
	// must establish whether it completed before another order is submitted.
	resp, err := service.client.UseCkafkaClient().HourModify(req)
	if err != nil {
		return fmt.Errorf("HourModify for %s failed; refresh and verify the instance before retrying: %w", d.Id(), err)
	}
	if resp == nil || resp.Response == nil {
		return fmt.Errorf("HourModify for %s returned an empty response; verify the instance before retrying", d.Id())
	}
	// Result={} is a valid accepted response. Completion is verified below.
	return service.waitCkafkaCapacity(ctx, d.Id(), target)
}

func (me *CkafkaService) waitCkafkaCapacity(ctx context.Context, id string, target ckafkaCapacity) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("Kafka capacity wait requires a deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return fmt.Errorf("Kafka capacity upgrade timed out before waiting for %s", id)
	}
	err := resource.RetryContext(ctx, remaining, func() *resource.RetryError {
		if err := ctx.Err(); err != nil {
			return resource.NonRetryableError(err)
		}
		req := ckafka.NewDescribeInstanceAttributesRequest()
		req.InstanceId = helper.String(id)
		ratelimit.Check(req.GetAction())
		resp, err := me.client.UseCkafkaClient().DescribeInstanceAttributes(req)
		if err != nil {
			return retryError(err)
		}
		if resp == nil || resp.Response == nil || resp.Response.Result == nil {
			return resource.RetryableError(fmt.Errorf("Kafka %s has no instance attributes yet", id))
		}
		info := resp.Response.Result
		if info.Status != nil && (*info.Status < 0 || *info.Status == 2 || *info.Status == 5) {
			return resource.NonRetryableError(fmt.Errorf("Kafka %s entered an unsuccessful state %d during the capacity upgrade", id, *info.Status))
		}
		capacity, err := ckafkaCapacityFromAttributes(info)
		if err != nil {
			return resource.RetryableError(err)
		}
		if info.Status != nil && *info.Status == 1 && capacity == target {
			return nil
		}
		return resource.RetryableError(fmt.Errorf("Kafka %s has not converged: target bandwidth=%d MB/s disk=%d GB topic=%d partition=%d; observed bandwidth=%d MB/s disk=%d GB topic=%d partition=%d", id, target.Bandwidth, target.DiskSize, target.Topic, target.Partition, capacity.Bandwidth, capacity.DiskSize, capacity.Topic, capacity.Partition))
	})
	if err != nil {
		return fmt.Errorf("waiting for Kafka %s capacity upgrade (target %d MB/s, %d GB): %w", id, target.Bandwidth, target.DiskSize, err)
	}
	return nil
}
