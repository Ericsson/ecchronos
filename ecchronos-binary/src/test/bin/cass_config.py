#!/usr/bin/env python3
# vi: syntax=python
#
# Copyright 2025 Telefonaktiebolaget LM Ericsson
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import global_variables as global_vars
from testcontainers.compose import DockerCompose, ComposeContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from typing import Optional
import docker
import os
from datetime import datetime, timedelta
from time import sleep
import logging
import subprocess
import json

DEFAULT_WAIT_TIME_IN_SECS = 60

# Budget for the initial cold bring-up of all nodes. Larger than the per-command
# timeout because newer Cassandra (6.0) is slow to accept JMX/converge gossip (~75s).
CLUSTER_STARTUP_WAIT_TIME_IN_SECS = 180

COMPOSE_FILE_NAME = "docker-compose.yml"
CASSANDRA_SEED_DC1_RC1_ND1 = "cassandra-seed-dc1-rack1-node1"

# All Cassandra node container names used by docker-compose, plus the optionally added
# extra node. Used when collecting per-node logs as test artifacts.
CASSANDRA_NODE_NAMES = [
    "cassandra-seed-dc1-rack1-node1",
    "cassandra-seed-dc2-rack1-node1",
    "cassandra-node-dc1-rack1-node2",
    "cassandra-node-dc2-rack1-node2",
]
CASSANDRA_EXTRA_NODE_NAME = "cassandra-node-dc1-rack1-node3"
ALTER_SYSTEM_AUTH_CQL = "ALTER KEYSPACE system_auth WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1, 'datacenter2': 2};"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
client = docker.from_env()


class CassandraCluster:
    def __init__(self, local):
        self.cassandra_compose: Optional[DockerCompose] = None
        self.node: Optional[ComposeContainer] = None
        self._extra_node: str = None
        self.local = local
        self.network: str = ""
        os.environ["CERTIFICATE_DIRECTORY"] = global_vars.CERTIFICATE_DIRECTORY
        os.environ["CASSANDRA_VERSION"] = global_vars.CASSANDRA_VERSION
        os.environ["JOLOKIA"] = global_vars.JOLOKIA_ENABLED
        os.environ["JOLOKIA_VERSION"] = global_vars.JOLOKIA_VERSION
        print(f"Using Jolokia agent version {global_vars.JOLOKIA_VERSION} (from pom property jolokia.adapter.version).")
        os.environ["PEM_ENABLED"] = global_vars.PEM_ENABLED
        os.environ["DOCKER_BUILDKIT"] = "1"
        self.cassandra_compose = DockerCompose(
            global_vars.CASSANDRA_DOCKER_COMPOSE_FILE_PATH,
            build=True,
            compose_file_name="docker-compose.yml",
            wait=False,
        )

    def create_cluster(self):
        try:
            self.cassandra_compose.start()
        except Exception as e:
            print(f"Error creating cluster: {e}")
            self.stop_cluster()
            raise e
        self._set_env()
        self._wait_for_nodes_to_be_up(4, CLUSTER_STARTUP_WAIT_TIME_IN_SECS * 1000)
        self._modify_system_auth_keyspace()
        self._run_full_repair()
        self._wait_for_auth_ready()
        self._setup_db()
        self._set_network()

    def _set_env(self):
        try:
            self.node = self.cassandra_compose.get_container(CASSANDRA_SEED_DC1_RC1_ND1)
            self.container_id = self.node.ID
            self.cassandra_ip = self._get_container_ip(self.container_id)
            self.cassandra_native_port = 9042
            self.cassandra_jmx_port = 7199
            self._create_cert_path()
            self._log_resolved_cassandra_version()
        except Exception as e:
            print(f"Error setting env: {e}")
            self.stop_cluster()
            raise e

    def _log_resolved_cassandra_version(self):
        # Report the actual version in use (useful when CASSANDRA_VERSION is 'latest').
        # Read CASSANDRA_VERSION from the base image 'cassandra:<tag>' rather than the built
        # container: the compose build arg shadows it on the container, but the base image
        # env holds the real version (e.g. tag '6.0' -> '6.0-alpha2').
        image = f"cassandra:{global_vars.CASSANDRA_VERSION}"
        result = subprocess.run(
            ["docker", "image", "inspect", "-f", "{{range .Config.Env}}{{println .}}{{end}}", image],
            capture_output=True,
            text=True,
        )
        version = "unknown"
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if line.startswith("CASSANDRA_VERSION="):
                    version = line.split("=", 1)[1].strip()
                    break
        logger.info("Cassandra version requested=%s, resolved=%s", global_vars.CASSANDRA_VERSION, version)

    def _set_network(self):
        result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format={{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}",
                self.container_id,
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        self.network = result.stdout.strip()

    def add_node(self):
        client.images.build(
            path=global_vars.CASSANDRA_DOCKER_COMPOSE_FILE_PATH,
            dockerfile=f"{global_vars.CASSANDRA_DOCKER_COMPOSE_FILE_PATH}/Dockerfile",
            tag="cassandra-node3:latest",
            buildargs={
                "CASSANDRA_VERSION": global_vars.CASSANDRA_VERSION,
                "JOLOKIA_VERSION": global_vars.JOLOKIA_VERSION,
            },
        )

        container_name = "cassandra-node-dc1-rack1-node3"
        cert_dir = global_vars.CERTIFICATE_DIRECTORY
        # '--ip' must go before the image name (docker treats args after the image as
        # the container command). It was previously appended after, so it was ignored.
        static_ip = "172.29.0.6"

        cmd = [
            "docker",
            "run",
            "-d",
            "--name",
            container_name,
            "--network",
            self.network,
            "--ip",
            static_ip,
            "-e",
            "CASSANDRA_CLUSTER_NAME=cassandra-cluster",
            "-e",
            "CASSANDRA_DC=datacenter1",
            "-e",
            "CASSANDRA_RACK=rack1",
            "-e",
            "CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch",
            "-e",
            "CASSANDRA_SEEDS=cassandra-seed-dc1-rack1-node1,cassandra-seed-dc2-rack1-node1,cassandra-node-dc1-rack1-node2,cassandra-node-dc2-rack1-node2",
            "-e",
            "CASSANDRA_PASSWORD_SEEDER=no",
            "-e",
            "CASSANDRA_PASSWORD=cassandra",
            "-e",
            f"MAX_HEAP_SIZE={os.environ.get('MAX_HEAP_SIZE', '3G')}",
            "-e",
            f"HEAP_NEWSIZE={os.environ.get('HEAP_NEWSIZE', '400M')}",
            "-e",
            "LOCAL_JMX=no",
            "-e",
            f"JOLOKIA={global_vars.JOLOKIA_ENABLED}",
            "-e",
            f"PEM_ENABLED={global_vars.PEM_ENABLED}",
            "-e",
            "JVM_EXTRA_OPTS=-Dcom.sun.management.jmxremote.authenticate=false "
            "-Dcassandra.superuser_setup_delay_ms=0 "
            "-Dcassandra.skip_wait_for_gossip_to_settle=0 "
            "-Dcassandra.ring_delay_ms=30000 "
            "-Dcassandra.write_survey=false",
            "-v",
            "cassandra-node3-data:/var/lib/cassandra",
            "-v",
            f"{cert_dir}:/etc/certificates",
            "cassandra-node3:latest",
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            new_container_id = result.stdout.strip()
            self._extra_node = new_container_id
        except subprocess.CalledProcessError as e:
            logger.error(f"Docker run failed with exit code {e.returncode}")
            logger.error(f"Command: {' '.join(cmd)}")
            logger.error(f"Stdout: {e.stdout}")
            logger.error(f"Stderr: {e.stderr}")
            raise

    @staticmethod
    def _create_cert_path():
        if not os.path.exists(global_vars.CASSANDRA_CERT_PATH):
            os.makedirs(global_vars.CASSANDRA_CERT_PATH)

    @staticmethod
    def _get_container_ip(container_id, network_name=None):
        client = docker.from_env()
        container = client.containers.get(container_id)
        networks = container.attrs["NetworkSettings"]["Networks"]

        if network_name:
            return networks[network_name]["IPAddress"]
        else:
            return next(iter(networks.values()))["IPAddress"]

    @staticmethod
    def _diagnose_container(container_id, tail=25):
        """Return a node container's running/exit/OOMKilled state plus last logs.

        Makes an OOM kill (exit 137 / OOMKilled=true) explicit instead of a bare
        count mismatch. Best-effort: never raises.
        """
        if not container_id:
            return "no extra-node container recorded"
        try:
            inspect = subprocess.run(
                [
                    "docker",
                    "inspect",
                    "-f",
                    "running={{.State.Running}} exitCode={{.State.ExitCode}} "
                    "oomKilled={{.State.OOMKilled}} status={{.State.Status}} error={{.State.Error}}",
                    container_id,
                ],
                capture_output=True,
                text=True,
            )
            if inspect.returncode != 0:
                return f"docker inspect failed (rc={inspect.returncode}): {inspect.stderr.strip()}"
            state = inspect.stdout.strip()

            logs = subprocess.run(
                ["docker", "logs", "--tail", str(tail), container_id],
                capture_output=True,
                text=True,
            )
            # Cassandra logs to stdout; include stderr too in case of early crashes.
            tail_output = (logs.stdout or "").strip() or (logs.stderr or "").strip()
            return f"{state}. last {tail} log line(s):\n{tail_output}"
        except Exception as e:  # noqa: BLE001
            return f"could not diagnose container {container_id}: {e}"

    @staticmethod
    def _parse_node_states(status_output):
        """Parse 'nodetool status' into (state, address) tuples.

        State is the 2-char token: U/D (up/down) + N/L/J/M (normal/leaving/joining/moving).
        Only UN is fully in service.
        """
        states = []
        valid_states = {"UN", "UJ", "UL", "UM", "DN", "DJ", "DL", "DM"}
        for line in status_output.splitlines():
            tokens = line.split()
            if len(tokens) >= 2 and tokens[0] in valid_states:
                states.append((tokens[0], tokens[1]))
        return states

    def _get_node_states(self):
        """Run 'nodetool status' and return parsed per-node states.

        Raises RuntimeError with command/rc/stdout/stderr on failure.
        """
        if global_vars.LOCAL != "true" and global_vars.PEM_ENABLED != "true":
            command = ["docker", "exec", self.container_id, "sh", "-c", "~/.cassandra/nodetool-status-ssl.sh"]
        else:
            command = ["docker", "exec", self.container_id, "bash", "-c", "nodetool -u cassandra -pw cassandra status"]

        try:
            process = subprocess.run(
                command,
                timeout=DEFAULT_WAIT_TIME_IN_SECS,
                encoding="utf-8",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except subprocess.TimeoutExpired as e:
            raise RuntimeError(
                f"'nodetool status' timed out after {DEFAULT_WAIT_TIME_IN_SECS}s. "
                f"Command: {' '.join(command)}. "
                f"stdout: {(e.stdout or '').strip()}. stderr: {(e.stderr or '').strip()}."
            ) from e

        if process.returncode != 0:
            raise RuntimeError(
                f"'nodetool status' failed with return code {process.returncode}. "
                f"Command: {' '.join(command)}. "
                f"stdout: {process.stdout.strip()}. stderr: {process.stderr.strip()}."
            )

        return self._parse_node_states(process.stdout)

    def verify_node_count(self, expected_nodes):
        try:
            states = self._get_node_states()
        except Exception as e:
            logger.error("Error verifying node count (nodetool status failed): %s", e)
            return False

        up_normal = [addr for state, addr in states if state == "UN"]
        if len(up_normal) == expected_nodes:
            return True

        other = [(state, addr) for state, addr in states if state != "UN"]
        logger.error(
            "Node count mismatch: expected %s Up/Normal (UN) node(s) but found %s. "
            "UN nodes: %s. Nodes in other states: %s. "
            "(A node present but not UN is typically still joining (UJ) or down (DN).)",
            expected_nodes,
            len(up_normal),
            up_normal,
            other if other else "none",
        )
        # Report the extra node's container state (it may have OOM-died, not just lagged).
        if self._extra_node is not None:
            logger.error("Extra-node container state: %s", self._diagnose_container(self._extra_node))
        return False

    def _wait_for_nodes_to_be_up(self, expected_nodes, max_wait_time_in_millis, required_stable_polls=2):
        # Require the UN count to hold for consecutive polls; a just-joined node can
        # flap UN -> UJ/DN, and returning on a single reading races verify_node_count().
        start_time = datetime.now()
        max_wait_time = timedelta(milliseconds=max_wait_time_in_millis)
        last_states = None
        last_error = None
        consecutive_ok = 0
        count_history = []

        while start_time + max_wait_time > datetime.now():
            try:
                sleep(5)
                last_states = self._get_node_states()
                last_error = None
                up_now = sum(1 for state, _ in last_states if state == "UN")
                count_history.append(up_now)
                if up_now == expected_nodes:
                    consecutive_ok += 1
                    if consecutive_ok >= required_stable_polls:
                        return
                else:
                    consecutive_ok = 0
            except Exception as e:
                # keep the latest error so the timeout can explain the root cause
                last_error = e
                consecutive_ok = 0
                count_history.append(None)  # None marks a poll where 'nodetool status' errored
                continue
        raise TimeoutError(
            f"Nodes did not reach {expected_nodes} Up/Normal (UN) after {max_wait_time_in_millis}ms. "
            f"Per-poll UN counts (None = nodetool errored): {count_history}. "
            f"Last observed states: {last_states if last_states is not None else 'unavailable'}. "
            f"Last error: {last_error if last_error is not None else 'none'}."
        )

    def _setup_db(self):
        command = ["docker", "exec", self.container_id, "bash", "/etc/cassandra/setup_db.sh"]

        result = subprocess.run(
            command,
            timeout=DEFAULT_WAIT_TIME_IN_SECS * 3,
            encoding="utf-8",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Surface setup_db.sh failures directly instead of letting them be masked
        # by the downstream keyspace-verification timeout, which makes the real
        # root cause (e.g. cqlsh auth errors) hard to diagnose.
        if result.returncode != 0:
            logger.warning(
                "setup_db.sh exited with return code %s. stdout: %s. stderr: %s",
                result.returncode,
                result.stdout.strip(),
                result.stderr.strip(),
            )

        # Verify ecchronos keyspace exists before proceeding
        self._verify_keyspace_exists("ecchronos")
        logger.info("Database setup completed and verified")

    def _verify_keyspace_exists(self, keyspace_name):
        """Verify that a keyspace exists and is available on all nodes"""
        max_attempts = 30
        last_returncode = None
        last_stdout = ""
        last_stderr = ""

        for attempt in range(max_attempts):
            try:
                command = ["docker", "exec", self.container_id, "cqlsh", "-e", f"DESCRIBE KEYSPACE {keyspace_name};"]
                result = subprocess.run(
                    command, timeout=10, encoding="utf-8", stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )

                last_returncode = result.returncode
                last_stdout = result.stdout.strip()
                last_stderr = result.stderr.strip()

                if result.returncode == 0 and keyspace_name in result.stdout:
                    logger.info(f"Keyspace {keyspace_name} verified on attempt {attempt + 1}")
                    return

                logger.warning(
                    f"Attempt {attempt + 1} to verify keyspace {keyspace_name} failed "
                    f"with return code {result.returncode}. stderr: {last_stderr}"
                )
            except Exception as e:
                last_stderr = str(e)
                logger.warning(f"Attempt {attempt + 1} to verify keyspace {keyspace_name} failed: {e}")

            sleep(2)

        raise TimeoutError(
            f"Keyspace {keyspace_name} not available after {max_attempts} attempts. "
            f"Last return code: {last_returncode}. "
            f"Last stderr: {last_stderr}. "
            f"Last stdout: {last_stdout}. "
            "Cassandra may be running, but the ecChronos keyspace was not available "
            "before the integration test continued."
        )

    def _modify_system_auth_keyspace(self):
        logger.info("Changing system_auth replication strategy")
        command = ["docker", "exec", self.container_id, "cqlsh", "-e", f"{ALTER_SYSTEM_AUTH_CQL}"]
        result = subprocess.run(
            command,
            timeout=DEFAULT_WAIT_TIME_IN_SECS * 3,
            encoding="utf-8",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            logger.warning(
                "Altering system_auth replication exited with return code %s. stdout: %s. stderr: %s",
                result.returncode,
                result.stdout.strip(),
                result.stderr.strip(),
            )

    def _run_full_repair(self):
        logger.info("Running Full Repair")
        if global_vars.LOCAL != "true" and global_vars.PEM_ENABLED != "true":
            command = [
                "docker",
                "exec",
                self.container_id,
                "bash",
                "-c",
                "nodetool --ssl -u cassandra -pw cassandra repair --full",
            ]
        else:
            command = [
                "docker",
                "exec",
                self.container_id,
                "bash",
                "-c",
                "nodetool -u cassandra -pw cassandra repair --full",
            ]

        result = subprocess.run(
            command,
            timeout=DEFAULT_WAIT_TIME_IN_SECS * 3,
            encoding="utf-8",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            logger.warning(
                "Full repair exited with return code %s. stdout: %s. stderr: %s",
                result.returncode,
                result.stdout.strip(),
                result.stderr.strip(),
            )

    def _wait_for_auth_ready(self):
        """Wait until the 'cassandra' superuser can actually authenticate.

        After raising the system_auth replication factor, the default superuser is
        read at QUORUM and its credentials may not yet be present on the newly
        required replicas, causing transient "Bad credentials" failures. Poll a
        trivial authenticated query until it succeeds so that setup_db.sh does not
        run against a cluster that cannot yet authenticate (which otherwise surfaces
        as a misleading "keyspace not found" timeout).

        Note: unlike the system_auth ALTER and full repair (which only log a warning
        on failure, since a single hiccup is often self-correcting), this method
        raises if authentication never succeeds. Proceeding without a working login
        cannot succeed and only produces the confusing downstream timeout, so failing
        fast here with an accurate message is intentional.
        """
        max_attempts = 20
        last_returncode = None
        last_stderr = ""

        for attempt in range(max_attempts):
            try:
                command = [
                    "docker",
                    "exec",
                    self.container_id,
                    "cqlsh",
                    "-u",
                    "cassandra",
                    "-p",
                    "cassandra",
                    "-e",
                    "SELECT now() FROM system.local;",
                ]
                result = subprocess.run(
                    command, timeout=10, encoding="utf-8", stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )

                last_returncode = result.returncode
                last_stderr = result.stderr.strip()

                if result.returncode == 0:
                    logger.info("Authentication ready on attempt %s", attempt + 1)
                    return

                logger.warning(
                    "Attempt %s to verify authentication failed with return code %s. stderr: %s",
                    attempt + 1,
                    result.returncode,
                    last_stderr,
                )
            except Exception as e:
                last_stderr = str(e)
                logger.warning("Attempt %s to verify authentication failed: %s", attempt + 1, e)

            sleep(3)

        raise TimeoutError(
            f"Authentication as user 'cassandra' not available after {max_attempts} attempts. "
            f"Last return code: {last_returncode}. Last stderr: {last_stderr}. "
            "The system_auth replication change may not have propagated (repair did not "
            "reconcile the superuser credentials) before the integration test continued."
        )

    def _nodes_for_log_collection(self):
        """Return [(label, container_ref), ...] for every live Cassandra node.

        Resolves the actual compose container IDs (compose prefixes service names with the
        project, so the bare service names are not valid 'docker cp' targets). Falls back to
        the known service names if the compose object cannot enumerate containers. Includes
        the extra node (by its container ID) when present.
        """
        nodes = []
        try:
            for container in self.cassandra_compose.get_containers():
                # Prefer the service name as the artifact label, the container ID as the ref.
                label = getattr(container, "Service", None) or getattr(container, "Name", None) or container.ID
                nodes.append((label, container.ID))
        except Exception as exc:  # noqa: BLE001  # pylint: disable=broad-except
            # Fall back to service names on any failure enumerating compose containers.
            logger.warning("Could not enumerate compose containers (%s); falling back to names.", exc)
            nodes = [(name, name) for name in CASSANDRA_NODE_NAMES]

        if self._extra_node is not None:
            nodes.append((CASSANDRA_EXTRA_NODE_NAME, self._extra_node))
        return nodes

    def collect_node_logs(self, destination_dir):
        """Copy the whole /var/log/cassandra directory from every node to the host.

        The Cassandra containers do not bind-mount their log directory, so the logs are
        lost on teardown. Over a long run Cassandra rotates system.log / debug.log into
        additional (and possibly compressed) segments such as system.log.1 or
        system.log.<date>.N.zip. Rather than assume rotation names, this copies the entire
        log directory per node so the live logs AND all rotated/archived segments are kept
        as artifacts. Best-effort: never raises; a node that is gone is skipped with a
        warning.

        Produces destination_dir/<service>/cassandra/<all log files>.
        """
        os.makedirs(destination_dir, exist_ok=True)
        for label, container_ref in self._nodes_for_log_collection():
            node_dir = os.path.join(destination_dir, label)
            dst = os.path.join(node_dir, "cassandra")
            os.makedirs(dst, exist_ok=True)
            # Trailing '/.' copies the directory contents into dst.
            src = f"{container_ref}:/var/log/cassandra/."
            result = subprocess.run(
                ["docker", "cp", src, dst],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0:
                collected = sorted(os.listdir(dst)) if os.path.isdir(dst) else []
                logger.info("Collected log dir from %s -> %s (%s files)", label, dst, len(collected))
                if collected:
                    logger.info("  %s: %s", label, collected)
            else:
                logger.warning(
                    "Could not collect log dir from %s (%s) (rc=%s): %s",
                    label,
                    container_ref,
                    result.returncode,
                    result.stderr.strip(),
                )

    def stop_cluster(self):
        # Tear down using the same compose command prefix the wrapper used for 'up'
        # (carries the correct -f/project so 'down' targets the images/volumes that
        # were actually created). '--rmi local' removes the locally-built compose
        # images so each run does not leave behind '<project>_cassandra-*' images.
        # The pulled base image (cassandra:X.Y) is kept, since only locally-built
        # images are removed.
        #
        # Note: this uses 'compose down --rmi local' rather than the name-based image
        # removal done in the Java harness (SharedCassandraCluster). It works here
        # because this is called from the pytest fixture teardown while the containers
        # are still up, so compose can still associate the built images with the
        # project. The Java shared-cluster path instead tears down at JVM exit, after
        # the testcontainers Ryuk reaper has removed the containers, at which point
        # 'compose down --rmi' is a no-op -- hence the different mechanism there.
        if self.cassandra_compose is not None:
            down_cmd = self.cassandra_compose.compose_command_property[:]
            down_cmd += ["down", "--volumes", "--remove-orphans", "--rmi", "local"]
            # Run in the compose context dir so the relative '-f docker-compose.yml'
            # resolves and the default project name matches the one used at 'up'.
            result = subprocess.run(
                down_cmd,
                capture_output=True,
                text=True,
                cwd=str(self.cassandra_compose.context),
            )
            if result.returncode != 0:
                logger.warning(
                    "compose down failed (rc=%s): %s. Falling back to wrapper stop().",
                    result.returncode,
                    result.stderr.strip(),
                )
                try:
                    self.cassandra_compose.stop(down=True)
                except Exception as e:
                    logger.error(f"Fallback compose stop() also failed: {e}")

    def run_cql(self, query):
        command = ["docker", "exec", self.container_id, "cqlsh", "-e", query]
        result = subprocess.run(
            command,
            timeout=DEFAULT_WAIT_TIME_IN_SECS,
            encoding="utf-8",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            raise RuntimeError(f"CQL query failed: {result.stderr}")
        return result.stdout

    def stop_extra_node(self):
        try:
            if self._extra_node != None:
                if global_vars.LOCAL != "true" and global_vars.PEM_ENABLED != "true":
                    command = [
                        "docker",
                        "exec",
                        self._extra_node,
                        "bash",
                        "-c",
                        "nodetool -u cassandra -pw cassandra --ssl decommission",
                    ]
                else:
                    command = [
                        "docker",
                        "exec",
                        self._extra_node,
                        "bash",
                        "-c",
                        "nodetool -u cassandra -pw cassandra decommission",
                    ]
                subprocess.run(command, capture_output=True, text=True, check=True)
                subprocess.run(["docker", "stop", self._extra_node], capture_output=True, text=True, check=True)
                subprocess.run(["docker", "rm", self._extra_node], capture_output=True, text=True, check=True)
                subprocess.run(
                    ["docker", "volume", "rm", "cassandra-node3-data"], capture_output=True, text=True, check=True
                )
                # Remove the locally-built extra-node image so a topology test run does not
                # leave behind 'cassandra-node3:latest'. This image is built directly in
                # add_node() and is not part of the compose project, so stop_cluster()'s
                # 'compose down --rmi local' does not remove it. Do not use check=True: if
                # the image is still referenced it must not fail teardown (warn only), and
                # the pulled base image (cassandra:X.Y) is untouched since only this tag is
                # removed.
                image_result = subprocess.run(
                    ["docker", "image", "rm", "-f", "cassandra-node3:latest"],
                    capture_output=True,
                    text=True,
                )
                if image_result.returncode != 0:
                    logger.warning(
                        "Failed to remove extra-node image cassandra-node3:latest (rc=%s): %s",
                        image_result.returncode,
                        image_result.stderr.strip(),
                    )
                self._extra_node = None
        except Exception as e:
            # Graceful teardown failed (node OOM-died or never joined, so can't decommission).
            # Log its state, then force-remove so it can't block the next run.
            logger.error("Error removing extra node: %s", e)
            logger.error("Extra-node container state: %s", self._diagnose_container(self._extra_node))
            self._force_remove_extra_node()

    def _force_remove_extra_node(self):
        """Force-remove the extra node and its volume. Best-effort: never raises."""
        if self._extra_node is not None:
            rm = subprocess.run(
                ["docker", "rm", "-f", self._extra_node],
                capture_output=True,
                text=True,
            )
            if rm.returncode != 0:
                logger.warning(
                    "Forced removal of extra-node container failed (rc=%s): %s",
                    rm.returncode,
                    rm.stderr.strip(),
                )
        vol = subprocess.run(
            ["docker", "volume", "rm", "-f", "cassandra-node3-data"],
            capture_output=True,
            text=True,
        )
        if vol.returncode != 0:
            logger.warning(
                "Forced removal of extra-node volume 'cassandra-node3-data' failed (rc=%s): %s",
                vol.returncode,
                vol.stderr.strip(),
            )
        self._extra_node = None
