# In this example we will launch MolDesignAgents on a remote endpoint
#

from __future__ import annotations

import argparse
import asyncio
import logging
import os

import dotenv
from academy.exchange import HttpExchangeFactory
from academy.identifier import AgentId
from academy.manager import Manager
from globus_compute_sdk import Executor as GlobusExecutor

from intermediate.custom_agents.monitored_md_agent import MonitoredMolDesignAgent

logger = logging.getLogger(__name__)


async def main(rounds: int, interval: float) -> int:

    # 1. Update this to match your preferred HPC system
    dotenv.load_dotenv(dotenv.find_dotenv("settings.env", usecwd=True))

    # 2. Go to dashboard.academy-agents.org to start a UserAgent and add
    #    to settings.env
    user_agent_id = os.environ["USER_AGENT_ID"]

    executor = GlobusExecutor(
        endpoint_id=os.environ["AURORA_ENDPOINT_ID"],
        # The batch job to request. The endpoint template must use these variables.
        # TODO: Update the following to match your preferred endpoint
        user_endpoint_config={
            "account": os.environ["AURORA_ACCOUNT"],
            # hpc-bridge reads the PBS queue from "partition"
            "partition": os.environ["AURORA_QUEUE"],
            # The agents stop after --rounds; keep rounds * interval below this
            "walltime": "00:30:00",
            "scheduler_options": "#PBS -l filesystems=home:flare",
            # Compute nodes reach the exchange and the MAG only through the ALCF proxy.
            # This replaces the template's worker_init, so activate the repo venv too.
            "worker_init": (
                "export http_proxy=http://proxy.alcf.anl.gov:3128"
                " https_proxy=http://proxy.alcf.anl.gov:3128;"
                " source $HOME/genesis_mission_hackathon/.venv/bin/activate"
            ),
        },
    )

    async with await Manager.from_exchange_factory(
        factory=HttpExchangeFactory(),
        executors=executor,
    ) as manager:
        # 3. Get a handle to the UserAgent
        agent_id = AgentId(uid=user_agent_id, name="UserAgent")
        user_agent_handle = manager.get_handle(agent_id)

        seeds = [
            "CC(=O)Oc1ccccc1C(=O)O",  # Aspirin
            "CC1=C(O)N=C(O)N1",  # 5-Methylhydantoin (enol form)
        ]

        # 4. We spawn agents that will process each molecule independently
        agents = []
        for molecule in seeds:
            agents.append(
                await manager.launch(
                    MonitoredMolDesignAgent,
                    args=(molecule,),
                    kwargs={
                        "llm_model": os.environ["LLM_MODEL"],
                        "base_url": os.environ["OPENAI_BASE_URL"],
                        "api_key": os.environ["AMSC_TOKEN"],
                        "user_agent_handle": user_agent_handle,
                    },
                ),
            )

        print("Starting discovery campaign")
        print("=" * 80)
        for _ in range(rounds):
            await asyncio.sleep(interval)
            for i, agent in enumerate(agents):
                print(f"Progress report from agent {i}")
                # A message on an exchange stream that is cut is not sent again,
                # so do not wait forever for this reply
                try:
                    report = await asyncio.wait_for(agent.report(), timeout=interval)
                except TimeoutError:
                    print(
                        "No reply this round: the batch job may still be in the queue, "
                        "or the message was lost"
                    )
                    continue
                print(f"Number of molecules simulated: {len(report)}")
                print(f"Five best molecules: {report[:5]}")
            print("=" * 80)

        for agent in agents:
            await agent.shutdown()

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rounds",
        type=int,
        default=40,
        help="Number of progress reports before shutting down",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=30,
        help="Seconds between progress reports",
    )
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main(args.rounds, args.interval)))
