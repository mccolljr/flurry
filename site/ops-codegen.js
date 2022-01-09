const {
  getCachedDocumentNodeFromSchema,
} = require("@graphql-codegen/plugin-helpers");
const { BaseTypesVisitor } = require("@graphql-codegen/visitor-plugin-common");
const gql = require("graphql");

/**
 * @typedef {{
 *    name: string;
 *    type: string;
 *    serde?: { to: (val: any) => string, from: (src: string) => any }
 * }} ScalarConfig
 *
 *
 * @typedef {{
 *    scalars: Record<string, ScalarConfig>
 * }} Config
 *
 * @typedef {{
 *    name: string;
 *    args: Record<string, string>;
 *    fields: Record<string, string>;
 * }} OutputObject
 *
 * @typedef {{
 *    name: string;
 *    fields: Record<string, string>
 * }} InputObject
 */

class Generator {
  constructor(schema, documents, config) {
    /** @type gql.GraphQLSchema */
    this.schema = schema;
    /** @type gql.DocumentNode */
    this.schema_ast = getCachedDocumentNodeFromSchema(schema);
    /** @type Array<{document: gql.DocumentNode, location: string}> */
    this.documents = documents;
    /** @type Config */
    this.config = config;
    /** @type string[] */
    this.file_items = [];
    /** @type Record<string, InputObject> */
    this.input_types = {};
    /** @type Record<string, OutputObject> */
    this.object_types = {};
  }

  addItem(/** @type string */ item) {
    this.file_items.push(item.trim());
  }

  generate() {
    this.processDocuments();
    return this.file_items.join("\n");
  }

  processDocuments() {
    this.processDocument(this.schema_ast);
    this.documents.forEach((doc) => this.processDocument(doc.document));
  }

  collectScalars(/** @type gql.DocumentNode */ doc) {
    const self = this;
    gql.visit(doc, {
      ScalarTypeDefinition(node) {},
      ObjectTypeDefinition(node) {},
      InputObjectTypeDefinition(node) {},
      EnumTypeDefinition(node) {},
    });
  }

  processDocument(/** @type gql.DocumentNode */ doc) {
    const self = this;
    gql.visit(doc, {
      ObjectTypeDefinition(node) {},
      ScalarTypeDefinition(node) {},
      OperationDefinition: {
        enter(node) {},
        leave(node) {
          self.addItem(
            `operations.${node.name.value ?? "AnonymousOperation"} = ${true};`
          );
        },
      },
    });
  }

  processDefinition(/** @type gql.DefinitionNode */ def) {
    if (gql.isExecutableDefinitionNode(def)) {
      def.variableDefinitions.forEach((vd) => {
        this.getTypeSpec(vd.type);
      });
    }
  }

  /** @returns { { spec: string, nullish: bool } } */
  getTypeSpec(/** @type gql.TypeNode */ typNode) {
    if (typNode.kind == gql.Kind.NAMED_TYPE) {
      return { spec: typNode.name.value, nullish: true };
    }
    if (typNode.kind == gql.Kind.NON_NULL_TYPE) {
      return { ...this.getTypeSpec(typNode.type), nullish: false };
    }
    if (typNode.kind == gql.Kind.LIST_TYPE) {
      const { spec, nullish } = this.getType(typNode.type);
      return new gql.GraphQLList(this.getType(typNode.type));
    }
    throw new Error(`unknown type kind: ${typNode.kind}`);
  }

  getTypeRepr(/** @type {{ spec: string, nullish: bool }} */ spec) {
    if (spec.nullish) {
      return `Maybe<${spec.spec}>`;
    }
    return spec.spec;
  }
}

module.exports = {
  plugin: (schema, documents, config) =>
    new Generator(schema, documents, config).generate(),
};
