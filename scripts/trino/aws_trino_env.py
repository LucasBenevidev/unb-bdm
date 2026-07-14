from __future__ import annotations

import argparse
import json
import os
import stat
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


PROJECT = "unb-bdm-trino"
DEFAULT_INSTANCE_TYPE = "m7i.2xlarge"
DEFAULT_VOLUME_SIZE_GB = 80
STATE_DIR = Path(".aws_trino")
STATE_FILE = STATE_DIR / "state.json"
KEY_FILE = STATE_DIR / f"{PROJECT}.pem"
KEY_NAME = f"{PROJECT}-key"
ROLE_NAME = f"{PROJECT}-ec2-role"
INSTANCE_PROFILE_NAME = f"{PROJECT}-instance-profile"
SECURITY_GROUP_NAME = f"{PROJECT}-sg"
INSTANCE_NAME = f"{PROJECT}-benchmark"
REGION_DEFAULT = "us-east-1"
BUCKET_DEFAULT = "unb-bdm-siorg"


def load_config() -> tuple[str, str]:
    load_dotenv(".env")
    return os.getenv("AWS_DEFAULT_REGION", REGION_DEFAULT), os.getenv("S3_BUCKET_NAME", BUCKET_DEFAULT)


def session(region: str) -> boto3.Session:
    return boto3.Session(region_name=region)


def read_state() -> dict[str, Any]:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def write_state(state: dict[str, Any]) -> None:
    STATE_DIR.mkdir(exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def public_ip_cidr() -> str:
    with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10) as response:
        ip = response.read().decode("utf-8").strip()
    return f"{ip}/32"


def latest_ubuntu_ami(ec2: Any) -> str:
    response = ec2.describe_images(
        Owners=["099720109477"],
        Filters=[
            {"Name": "name", "Values": ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]},
            {"Name": "architecture", "Values": ["x86_64"]},
            {"Name": "root-device-type", "Values": ["ebs"]},
            {"Name": "virtualization-type", "Values": ["hvm"]},
        ],
    )
    images = sorted(response["Images"], key=lambda item: item["CreationDate"], reverse=True)
    if not images:
        raise RuntimeError("No Ubuntu 22.04 AMI found.")
    return images[0]["ImageId"]


def default_subnet(ec2: Any) -> tuple[str, str]:
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])["Vpcs"]
    if not vpcs:
        raise RuntimeError("No default VPC found.")
    vpc_id = vpcs[0]["VpcId"]
    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])["Subnets"]
    if not subnets:
        raise RuntimeError("No subnet found in default VPC.")
    subnets = sorted(subnets, key=lambda item: item.get("AvailableIpAddressCount", 0), reverse=True)
    return vpc_id, subnets[0]["SubnetId"]


def ensure_key_pair(ec2: Any) -> None:
    STATE_DIR.mkdir(exist_ok=True)
    if KEY_FILE.exists():
        return
    try:
        ec2.delete_key_pair(KeyName=KEY_NAME)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "InvalidKeyPair.NotFound":
            raise
    response = ec2.create_key_pair(KeyName=KEY_NAME, KeyType="rsa", KeyFormat="pem")
    KEY_FILE.write_text(response["KeyMaterial"], encoding="utf-8")
    try:
        KEY_FILE.chmod(stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass


def ensure_security_group(ec2: Any, vpc_id: str, ssh_cidr: str) -> str:
    groups = ec2.describe_security_groups(
        Filters=[
            {"Name": "group-name", "Values": [SECURITY_GROUP_NAME]},
            {"Name": "vpc-id", "Values": [vpc_id]},
        ]
    )["SecurityGroups"]
    if groups:
        group_id = groups[0]["GroupId"]
    else:
        group_id = ec2.create_security_group(
            GroupName=SECURITY_GROUP_NAME,
            Description="Temporary Trino benchmark access",
            VpcId=vpc_id,
            TagSpecifications=[
                {
                    "ResourceType": "security-group",
                    "Tags": [{"Key": "Project", "Value": PROJECT}, {"Key": "Name", "Value": SECURITY_GROUP_NAME}],
                }
            ],
        )["GroupId"]
    try:
        ec2.authorize_security_group_ingress(
            GroupId=group_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": ssh_cidr, "Description": "SSH for benchmark operator"}],
                }
            ],
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "InvalidPermission.Duplicate":
            raise
    return group_id


def ensure_role(iam: Any, bucket: str) -> None:
    assume = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    try:
        iam.get_role(RoleName=ROLE_NAME)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchEntity":
            raise
        iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(assume),
            Tags=[{"Key": "Project", "Value": PROJECT}],
        )
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                "Resource": [f"arn:aws:s3:::{bucket}"],
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket}/distribuicao/*",
                    f"arn:aws:s3:::{bucket}/estrutura-organizacional-completa/*",
                    f"arn:aws:s3:::{bucket}/iceberg/*",
                ],
            },
            {
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:CreateDatabase",
                    "glue:UpdateDatabase",
                    "glue:DeleteDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:CreatePartition",
                    "glue:UpdatePartition",
                    "glue:DeletePartition",
                    "glue:BatchCreatePartition",
                    "glue:BatchDeletePartition",
                    "glue:BatchGetPartition",
                ],
                "Resource": "*",
            },
        ],
    }
    iam.put_role_policy(RoleName=ROLE_NAME, PolicyName=f"{PROJECT}-s3-glue", PolicyDocument=json.dumps(policy))
    iam.attach_role_policy(RoleName=ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore")
    try:
        iam.get_instance_profile(InstanceProfileName=INSTANCE_PROFILE_NAME)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchEntity":
            raise
        iam.create_instance_profile(InstanceProfileName=INSTANCE_PROFILE_NAME)
    try:
        iam.add_role_to_instance_profile(InstanceProfileName=INSTANCE_PROFILE_NAME, RoleName=ROLE_NAME)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "LimitExceeded":
            raise
    time.sleep(15)


def user_data(region: str, bucket: str) -> str:
    return f"""#!/bin/bash
set -euxo pipefail
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io docker-compose-plugin python3-venv python3-pip git unzip jq
systemctl enable --now docker
usermod -aG docker ubuntu
mkdir -p /opt/unb-bdm
cat >/etc/profile.d/unb-bdm.sh <<'EOF'
export AWS_DEFAULT_REGION={region}
export S3_BUCKET_NAME={bucket}
export TRINO_HOST=localhost
export TRINO_PORT=8090
export TRINO_USER=cassio
export TRINO_CATALOG=iceberg
export TRINO_REQUEST_TIMEOUT=600
EOF
"""


def provision(args: argparse.Namespace) -> int:
    region, bucket = load_config()
    aws = session(region)
    ec2 = aws.client("ec2")
    iam = aws.client("iam")
    state = read_state()
    if state.get("instance_id"):
        print(json.dumps(state, indent=2))
        return 0
    ssh_cidr = args.ssh_cidr or public_ip_cidr()
    vpc_id, subnet_id = default_subnet(ec2)
    ensure_key_pair(ec2)
    group_id = ensure_security_group(ec2, vpc_id, ssh_cidr)
    ensure_role(iam, bucket)
    ami_id = latest_ubuntu_ami(ec2)
    response = ec2.run_instances(
        ImageId=ami_id,
        InstanceType=args.instance_type,
        MinCount=1,
        MaxCount=1,
        KeyName=KEY_NAME,
        SecurityGroupIds=[group_id],
        SubnetId=subnet_id,
        IamInstanceProfile={"Name": INSTANCE_PROFILE_NAME},
        UserData=user_data(region, bucket),
        BlockDeviceMappings=[
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "VolumeSize": args.volume_size,
                    "VolumeType": "gp3",
                    "DeleteOnTermination": True,
                },
            }
        ],
        TagSpecifications=[
            {
                "ResourceType": "instance",
                "Tags": [{"Key": "Name", "Value": INSTANCE_NAME}, {"Key": "Project", "Value": PROJECT}],
            }
        ],
    )
    instance_id = response["Instances"][0]["InstanceId"]
    waiter = ec2.get_waiter("instance_running")
    waiter.wait(InstanceIds=[instance_id])
    desc = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]
    state = {
        "region": region,
        "bucket": bucket,
        "instance_id": instance_id,
        "instance_type": args.instance_type,
        "volume_size_gb": args.volume_size,
        "key_file": str(KEY_FILE),
        "security_group_id": group_id,
        "subnet_id": subnet_id,
        "public_dns": desc.get("PublicDnsName"),
        "public_ip": desc.get("PublicIpAddress"),
        "ssh_user": "ubuntu",
    }
    write_state(state)
    print(json.dumps(state, indent=2))
    print(f"SSH: ssh -i {KEY_FILE} ubuntu@{state['public_dns'] or state['public_ip']}")
    return 0


def preflight(args: argparse.Namespace) -> int:
    region, bucket = load_config()
    aws = session(region)
    ec2 = aws.client("ec2")
    ami_id = latest_ubuntu_ami(ec2)
    _, subnet_id = default_subnet(ec2)
    instance_type = args.instance_type
    info = ec2.describe_instance_types(InstanceTypes=[instance_type])["InstanceTypes"][0]
    print(
        json.dumps(
            {
                "region": region,
                "bucket": bucket,
                "instance_type": instance_type,
                "vcpu": info["VCpuInfo"]["DefaultVCpus"],
                "memory_mib": info["MemoryInfo"]["SizeInMiB"],
                "ami_id": ami_id,
                "subnet_id": subnet_id,
            },
            indent=2,
        )
    )
    try:
        ec2.run_instances(
            ImageId=ami_id,
            InstanceType=instance_type,
            MinCount=1,
            MaxCount=1,
            SubnetId=subnet_id,
            DryRun=True,
        )
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        message = exc.response["Error"].get("Message", "")
        if code == "DryRunOperation":
            print("OK: RunInstances dry-run autorizado.")
            return 0
        print(f"FAIL: RunInstances dry-run bloqueado: {code}: {message}")
        return 1
    return 0


def status(_: argparse.Namespace) -> int:
    region, _ = load_config()
    state = read_state()
    if not state.get("instance_id"):
        print("No instance in local state.")
        return 1
    ec2 = session(region).client("ec2")
    desc = ec2.describe_instances(InstanceIds=[state["instance_id"]])["Reservations"][0]["Instances"][0]
    state.update(
        {
            "state": desc["State"]["Name"],
            "public_dns": desc.get("PublicDnsName"),
            "public_ip": desc.get("PublicIpAddress"),
        }
    )
    write_state(state)
    print(json.dumps(state, indent=2))
    return 0


def cleanup(_: argparse.Namespace) -> int:
    region, _ = load_config()
    aws = session(region)
    ec2 = aws.client("ec2")
    iam = aws.client("iam")
    state = read_state()
    if state.get("instance_id"):
        print("Local state still has an instance. Run terminate first.")
        return 1
    try:
        groups = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [SECURITY_GROUP_NAME]}])[
            "SecurityGroups"
        ]
        for group in groups:
            ec2.delete_security_group(GroupId=group["GroupId"])
            print(f"Deleted security group {group['GroupId']}.")
    except ClientError as exc:
        print(f"Security group cleanup skipped: {exc}")
    try:
        ec2.delete_key_pair(KeyName=KEY_NAME)
        print(f"Deleted key pair {KEY_NAME}.")
    except ClientError as exc:
        print(f"Key pair cleanup skipped: {exc}")
    if KEY_FILE.exists():
        KEY_FILE.unlink()
        print(f"Deleted local key file {KEY_FILE}.")
    try:
        iam.remove_role_from_instance_profile(InstanceProfileName=INSTANCE_PROFILE_NAME, RoleName=ROLE_NAME)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchEntity":
            print(f"Instance profile role removal skipped: {exc}")
    try:
        iam.delete_instance_profile(InstanceProfileName=INSTANCE_PROFILE_NAME)
        print(f"Deleted instance profile {INSTANCE_PROFILE_NAME}.")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchEntity":
            print(f"Instance profile cleanup skipped: {exc}")
    try:
        iam.detach_role_policy(RoleName=ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchEntity":
            print(f"Role detach skipped: {exc}")
    try:
        iam.delete_role_policy(RoleName=ROLE_NAME, PolicyName=f"{PROJECT}-s3-glue")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchEntity":
            print(f"Role inline policy cleanup skipped: {exc}")
    try:
        iam.delete_role(RoleName=ROLE_NAME)
        print(f"Deleted role {ROLE_NAME}.")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchEntity":
            print(f"Role cleanup skipped: {exc}")
    if STATE_FILE.exists():
        STATE_FILE.unlink()
    return 0


def terminate(_: argparse.Namespace) -> int:
    region, _ = load_config()
    state = read_state()
    if not state.get("instance_id"):
        print("No instance in local state.")
        return 0
    ec2 = session(region).client("ec2")
    ec2.terminate_instances(InstanceIds=[state["instance_id"]])
    ec2.get_waiter("instance_terminated").wait(InstanceIds=[state["instance_id"]])
    print(f"Terminated {state['instance_id']}.")
    return 0


def commands(_: argparse.Namespace) -> int:
    state = read_state()
    if not state.get("public_dns") and not state.get("public_ip"):
        print("Run provision/status first.")
        return 1
    host = state.get("public_dns") or state["public_ip"]
    key = state["key_file"]
    print(f"ssh -i {key} ubuntu@{host}")
    print(f"scp -i {key} -r . ubuntu@{host}:/opt/unb-bdm")
    print(f"scp -i {key} -r ubuntu@{host}:/opt/unb-bdm/results/trino_aws_fixed results/")
    print(f"python scripts/trino/aws_trino_env.py terminate")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Provisiona EC2 temporaria para benchmark Trino/Iceberg.")
    sub = parser.add_subparsers(dest="command", required=True)
    p_preflight = sub.add_parser("preflight")
    p_preflight.add_argument("--instance-type", default=DEFAULT_INSTANCE_TYPE)
    p_preflight.set_defaults(func=preflight)
    p_provision = sub.add_parser("provision")
    p_provision.add_argument("--instance-type", default=DEFAULT_INSTANCE_TYPE)
    p_provision.add_argument("--volume-size", type=int, default=DEFAULT_VOLUME_SIZE_GB)
    p_provision.add_argument("--ssh-cidr", default=None, help="CIDR autorizado para SSH. Default: IP publico atual /32.")
    p_provision.set_defaults(func=provision)
    p_status = sub.add_parser("status")
    p_status.set_defaults(func=status)
    p_cleanup = sub.add_parser("cleanup")
    p_cleanup.set_defaults(func=cleanup)
    p_terminate = sub.add_parser("terminate")
    p_terminate.set_defaults(func=terminate)
    p_commands = sub.add_parser("commands")
    p_commands.set_defaults(func=commands)
    args = parser.parse_args()
    try:
        return args.func(args)
    except ClientError as exc:
        print(f"AWS error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
