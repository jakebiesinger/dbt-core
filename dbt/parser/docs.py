import dbt.exceptions
from dbt.node_types import NodeType
from dbt.parser.base import BaseParser
from dbt.contracts.graph.unparsed import UnparsedDocumentationFile
from dbt.contracts.graph.parsed import ParsedDocumentation

import jinja2.runtime
import os


class DocumentationParser(BaseParser):
    @classmethod
    def load_file(cls, package_name, root_dir, relative_dirs):
        """Load and parse documentation in a list of projects. Returns a list
        of ParsedNodes.
        """

        extensions = ["[!.#~]*.md", "[!.#~]*.sql"]

        file_matches = []
        for extension in extensions:
            file_matches.extend(
                dbt.clients.system.find_matching(
                    root_dir, relative_dirs, extension))

        for file_match in file_matches:
            file_contents = dbt.clients.system.load_file_contents(
                file_match.get('absolute_path'), strip=False)

            parts = dbt.utils.split_path(file_match.get('relative_path', ''))
            name, _ = os.path.splitext(parts[-1])

            path = file_match.get('relative_path')
            original_file_path = os.path.join(
                file_match.get('searched_path'),
                path)

            yield UnparsedDocumentationFile(
                root_path=root_dir,
                resource_type=NodeType.Documentation,
                path=path,
                original_file_path=original_file_path,
                package_name=package_name,
                file_contents=file_contents
            )

    @classmethod
    def parse(cls, all_projects, root_project_config, docfile):
        env = dbt.clients.jinja.get_env()
        ast = env.parse(docfile.file_contents)

        for macro in ast.find_all(jinja2.nodes.Macro):
            key = macro.name
            if not key.startswith(dbt.utils.DOCS_PREFIX):
                continue

            name = key.replace(dbt.utils.DOCS_PREFIX, '')

            # because docs are in their own graph namespace, node type doesn't
            # need to be part of the unique ID.
            unique_id = '{}.{}'.format(docfile.package_name, name)
            fqn = cls.get_fqn(docfile.path,
                              all_projects[docfile.package_name])

            # HACK: Assume that docs macros do not call other macros and only
            # have a single relevant Output node. Alternatively we could render
            # this particular block.
            if macro.body and macro.body[0].nodes:
                contents = macro.body[0].nodes[0].data
            else:
                contents = ''
            merged = dbt.utils.deep_merge(
                docfile.serialize(),
                {
                    'name': name,
                    'unique_id': unique_id,
                    'block_contents': contents,
                }
            )
            yield ParsedDocumentation(**merged)

    @classmethod
    def load_and_parse(cls, package_name, root_project, all_projects, root_dir,
                       relative_dirs):
        to_return = {}
        for docfile in cls.load_file(package_name, root_dir, relative_dirs):
                for parsed in cls.parse(all_projects, root_project, docfile):
                    if parsed.unique_id in to_return:
                        dbt.exceptions.raise_duplicate_resource_name(
                            to_return[parsed.unique_id], parsed
                        )
                    to_return[parsed.unique_id] = parsed
        return to_return
