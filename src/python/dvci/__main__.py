#!/usr/bin/env python3

import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

import jinja2
import rich_click
import ruamel.yaml as yaml
from dvc.repo import Repo
from dvc.stage import PipelineStage
from jinja2 import Environment, PackageLoader
from rich.logging import RichHandler

TEMPLATE_PATH = "/usr/local/share/dvci/templates"


LOG = logging.getLogger(__name__)


@rich_click.group()
def main():
    logging.basicConfig(level="INFO", handlers=[RichHandler()])
    pass


@rich_click.command()
@rich_click.option("--image-tag", default="latest", type=str)
@rich_click.option("--repo-path", default=None, type=Optional[Path])
@rich_click.argument("pipeine_file")
def build_ci(
    pipeline_file: Path,
    repo_path: Optional[Path] = None,
    image_tag: Optional[str] = None,
):
    if image_tag is None:
        image_tag = "latest"
    dvc_repo = Repo(str(repo_path))
    repo_root = str(Path(dvc_repo.root_dir).relative_to(dvc_repo.scm.root_dir))
    job_dependencies = _collect_dependencies(dvc_repo)
    dvc_jobs = {
        j.name: {
            "job": {
                "name": j.name,
                "dependencies": job_dependencies[j.name],
                "inputs": [d.def_path for d in j.deps],
            },
            "dvc": {
                "root": repo_root,
                "dvci_tag": image_tag,
            },
            "meta": j.meta["dvci"],
        }
        for j in dvc_repo.stage.load_all()
        if isinstance(j, PipelineStage)
        and j.meta is not None
        and "dvci" in j.meta
        and j.name is not None
    }
    jinja_environment = Environment(
        loader=PackageLoader(__name__.split(".")[0]), autoescape=jinja2.select_autoescape()
    )
    with pipeline_file.open("w") as output_file:
        print("stages:\n  - dvci\n", file=output_file)
        for name, context in dvc_jobs.items():
            runner = context["meta"].get("runner", "local")
            template = jinja_environment.get_template(f"{runner}.yaml.jinja")
            print(template.render(**context), file=output_file)
            print(file=output_file)
            LOG.info(f"Processed {name}")
        final_job = {
            "job": {
                "name": "@@final",
                "dependencies": _collect_final_jobs(dvc_repo),
                "inputs": [],
            },
            "dvc": {
                "root": repo_root,
                "dvci_tag": image_tag,
            },
            "meta": {},
        }
        template = jinja_environment.get_template(".final.yaml.jinja")
        print(template.render(**final_job), file=output_file)
        print(file=output_file)


def _collect_final_jobs(dvc_repo: Repo) -> List[str]:
    index = dvc_repo.index
    stages = set()
    for j in index.stages:
        if (
            not isinstance(j, PipelineStage)
            or j.meta is None
            or "dvci" not in j.meta
            or j.name is None
        ):
            continue
        stages.add(j.name)
    dependencies = set(d.name for _0, d in index.graph.edges if isinstance(d, PipelineStage))
    return list(stages - dependencies)


@rich_click.command()
@rich_click.option("--repo-path", default=None, type=Optional[Path])
def merge_inputs(repo_path: Optional[Path] = None):
    dvc_repo = Repo(str(repo_path))
    repo_root = Path(dvc_repo.root_dir)
    job_dependencies = _collect_dependencies(dvc_repo)
    collected = {
        "schema": "2.0",
        "stages": {},
    }
    for path in repo_root.glob("dvc/*"):
        with path.open("r") as yaml_file:
            content = yaml.safe_load(yaml_file).get("stages", {})
        dependents = {path.stem}
        while dependents:
            job_name = dependents.pop()
            if job_name in content:
                collected["stages"][job_name] = content[job_name]
                LOG.info(f"Merged Job {job_name} from file {path.name}")
            dependents.update(job_dependencies.get(job_name, []))
    with (repo_root / "dvc.lock").open("w") as output_file:
        yaml.safe_dump(collected, output_file)


def _collect_dependencies(dvc_repo: Repo) -> Dict[str, List[str]]:
    job_dependencies = defaultdict(list)
    for source_stage, dependent_stage in dvc_repo.index.graph.edges:
        if not isinstance(source_stage, PipelineStage):
            continue
        if not isinstance(dependent_stage, PipelineStage):
            continue
        if dependent_stage.meta is not None and "dvci" in dependent_stage.meta:
            assert isinstance(source_stage.name, str)
            assert isinstance(dependent_stage.name, str)
            job_dependencies[source_stage.name].append(dependent_stage.name)
    LOG.info(f"Resolved job dependencies")
    return job_dependencies


main.add_command(build_ci)
main.add_command(merge_inputs)

if __name__ == "__main__":
    main()
