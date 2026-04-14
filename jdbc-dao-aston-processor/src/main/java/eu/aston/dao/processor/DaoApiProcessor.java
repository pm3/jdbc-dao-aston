package eu.aston.dao.processor;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@SupportedAnnotationTypes("eu.aston.dao.DaoApi")
@SupportedSourceVersion(SourceVersion.RELEASE_17)
public class DaoApiProcessor extends AbstractProcessor {

    private final Set<String> generatedServiceEntries = new LinkedHashSet<>();

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (TypeElement annotation : annotations) {
            for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
                if (element instanceof TypeElement typeElement && typeElement.getKind() == ElementKind.INTERFACE) {
                    try {
                        processInterface(typeElement);
                    } catch (IOException e) {
                        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                                "Failed to generate DAO impl: " + e.getMessage(), element);
                    }
                }
            }
        }

        if (roundEnv.processingOver() && !generatedServiceEntries.isEmpty()) {
            writeServiceFile();
        }

        return true;
    }

    private void processInterface(TypeElement iface) throws IOException {
        String packageName = getPackageName(iface);
        String simpleName = iface.getSimpleName().toString();
        String implName = simpleName + "$Impl";
        String qualifiedImpl = packageName.isEmpty() ? implName : packageName + "." + implName;

        // Find EntityConfig fields
        List<EntityConfigInfo> entityConfigs = findEntityConfigs(iface);

        // Find methods to implement
        List<MethodInfo> methods = findMethods(iface);

        JavaFileObject sourceFile = processingEnv.getFiler().createSourceFile(qualifiedImpl, iface);
        try (Writer writer = sourceFile.openWriter();
                BufferedWriter bw = new BufferedWriter(writer)) {
            generateImpl(bw, packageName, simpleName, implName, entityConfigs, methods);
        }

        generatedServiceEntries.add(qualifiedImpl);
    }

    // --- Entity config scanning ---

    private List<EntityConfigInfo> findEntityConfigs(TypeElement iface) {
        var configs = new ArrayList<EntityConfigInfo>();
        for (Element e : iface.getEnclosedElements()) {
            if (e.getKind() != ElementKind.FIELD)
                continue;
            if (!e.getModifiers().contains(Modifier.STATIC))
                continue;
            TypeMirror fieldType = e.asType();
            if (fieldType.getKind() != TypeKind.DECLARED)
                continue;
            DeclaredType dt = (DeclaredType) fieldType;
            String rawName = ((TypeElement) dt.asElement()).getQualifiedName().toString();
            if (!"eu.aston.dao.EntityConfig".equals(rawName))
                continue;
            if (dt.getTypeArguments().isEmpty())
                continue;

            TypeMirror typeArg = dt.getTypeArguments().get(0);
            String entityType = typeArg.toString();
            String fieldName = e.getSimpleName().toString();
            configs.add(new EntityConfigInfo(fieldName, entityType));
        }
        return configs;
    }

    // --- Method scanning ---

    private List<MethodInfo> findMethods(TypeElement iface) {
        var methods = new ArrayList<MethodInfo>();
        for (Element e : iface.getEnclosedElements()) {
            if (e.getKind() != ElementKind.METHOD)
                continue;
            ExecutableElement method = (ExecutableElement) e;
            if (method.isDefault())
                continue;
            if (method.getModifiers().contains(Modifier.STATIC))
                continue;

            String name = method.getSimpleName().toString();
            TypeMirror returnType = method.getReturnType();
            List<ParamInfo> params = new ArrayList<>();
            for (VariableElement param : method.getParameters()) {
                params.add(new ParamInfo(param.getSimpleName().toString(), param.asType()));
            }

            // Check for @Query annotation
            String queryValue = null;
            for (var ann : method.getAnnotationMirrors()) {
                String annName = ann.getAnnotationType().toString();
                if ("eu.aston.dao.Query".equals(annName)) {
                    for (var entry : ann.getElementValues().entrySet()) {
                        if ("value()".equals(entry.getKey().toString())) {
                            queryValue = entry.getValue().getValue().toString();
                        }
                    }
                }
            }

            methods.add(new MethodInfo(name, returnType, returnType, params, queryValue, method));
        }
        return methods;
    }

    // --- Code generation ---

    private void generateImpl(BufferedWriter bw, String packageName, String ifaceName, String implName,
            List<EntityConfigInfo> configs, List<MethodInfo> methods) throws IOException {
        if (!packageName.isEmpty()) {
            bw.write("package " + packageName + ";\n\n");
        }

        bw.write("import eu.aston.dao.impl.DaoBase;\n");
        bw.write("import eu.aston.dao.DaoProvider;\n");
        bw.write("import eu.aston.dao.EntityConfig;\n");
        bw.write("import javax.sql.DataSource;\n");
        bw.write("import java.util.List;\n");
        bw.write("import java.util.Optional;\n\n");

        bw.write("public class " + implName + " extends DaoBase implements " + ifaceName + ", DaoProvider {\n\n");

        // No-arg constructor (for ServiceLoader)
        bw.write("    public " + implName + "() {}\n\n");

        // Functional constructor
        bw.write("    public " + implName + "(DataSource dataSource, Object objectMapper) {\n");
        bw.write("        this.dataSource = dataSource;\n");
        bw.write("        this.objectMapper = (com.fasterxml.jackson.databind.ObjectMapper) objectMapper;\n");
        bw.write("    }\n\n");

        // DaoProvider methods
        bw.write("    @Override\n");
        bw.write("    public Class<?> daoInterface() { return " + ifaceName + ".class; }\n\n");

        bw.write("    @Override\n");
        bw.write("    public Object newInstance(DataSource ds, Object om) {\n");
        bw.write("        return new " + implName + "(ds, om);\n");
        bw.write("    }\n\n");

        // Generate each method
        for (MethodInfo method : methods) {
            generateMethod(bw, method, configs, ifaceName);
        }

        bw.write("}\n");
    }

    private void generateMethod(BufferedWriter bw, MethodInfo method, List<EntityConfigInfo> configs, String ifaceName)
            throws IOException {
        String returnTypeStr = toTypeString(method.returnType);
        String methodName = method.name;

        // Static QueryParam map for @Query methods with params
        boolean hasParams = method.queryValue != null && !method.params.isEmpty();
        if (hasParams) {
            boolean expandBean = method.params.size() == 1 && isBeanType(method.params.get(0).type);
            String ppExpr = expandBean ? buildBeanQueryParamMapExpr(method.params.get(0))
                    : buildQueryParamMapExpr(method.params);
            bw.write("    private static final java.util.Map<String, eu.aston.dao.impl.QueryParam> pp$" + methodName
                    + " = " + ppExpr + ";\n");
        }

        bw.write("    @Override\n");
        bw.write("    public " + returnTypeStr + " " + methodName + "(");
        for (int i = 0; i < method.params.size(); i++) {
            if (i > 0)
                bw.write(", ");
            bw.write(toTypeString(method.params.get(i).type) + " " + method.params.get(i).name);
        }
        bw.write(") {\n");

        if (method.queryValue != null) {
            generateQueryMethodBody(bw, method, hasParams);
        } else {
            generateEntityMethodBody(bw, method, configs, ifaceName);
        }

        bw.write("    }\n\n");
    }

    private void generateQueryMethodBody(BufferedWriter bw, MethodInfo method, boolean hasParams) throws IOException {
        String sql = escapeJava(method.queryValue);
        ReturnKind kind = resolveReturnKind(method.returnType);
        String[] elementTypeInfo = resolveElementType(method.returnType);
        String elementType = elementTypeInfo[0];

        String ppExpr = hasParams ? "pp$" + method.name : "java.util.Map.of()";
        String argsExpr;
        if (!hasParams) {
            argsExpr = "null";
        } else {
            boolean expandBean = method.params.size() == 1 && isBeanType(method.params.get(0).type);
            argsExpr = expandBean ? "expandBeanArgs(" + ppExpr + ", " + method.params.get(0).name + ")"
                    : "new Object[]{" + joinParamValues(method.params) + "}";
        }

        switch (kind) {
            case VOID -> bw.write("        queryExecute(\"" + sql + "\", " + ppExpr + ", " + argsExpr + ");\n");
            case INT -> bw.write("        return queryUpdate(\"" + sql + "\", " + ppExpr + ", " + argsExpr + ");\n");
            case ONE -> bw.write("        return queryOne(" + elementType + ".class, \"" + sql + "\", " + ppExpr + ", "
                    + argsExpr + ");\n");
            case OPTIONAL -> bw.write("        return queryOptional(" + elementType + ".class, \"" + sql + "\", "
                    + ppExpr + ", " + argsExpr + ");\n");
            case LIST -> bw.write("        return queryList(" + elementType + ".class, \"" + sql + "\", " + ppExpr
                    + ", " + argsExpr + ");\n");
        }
    }

    private void generateEntityMethodBody(BufferedWriter bw, MethodInfo method, List<EntityConfigInfo> configs,
            String ifaceName) throws IOException {
        String name = method.name;
        if (name.startsWith("load")) {
            // Return type determines entity
            String returnTypeRaw = toRawTypeString(method.returnType);
            String configField = findConfigField(configs, returnTypeRaw);
            bw.write("        return entityLoad(" + ifaceName + "." + configField + ", " + method.params.get(0).name
                    + ");\n");
        } else if (name.startsWith("insert")) {
            String paramTypeRaw = toRawTypeString(method.params.get(0).type);
            String configField = findConfigField(configs, paramTypeRaw);
            bw.write("        entityInsert(" + ifaceName + "." + configField + ", " + method.params.get(0).name
                    + ");\n");
        } else if (name.startsWith("update")) {
            String paramTypeRaw = toRawTypeString(method.params.get(0).type);
            String configField = findConfigField(configs, paramTypeRaw);
            bw.write("        entityUpdate(" + ifaceName + "." + configField + ", " + method.params.get(0).name
                    + ");\n");
        } else if (name.startsWith("save")) {
            String paramTypeRaw = toRawTypeString(method.params.get(0).type);
            String configField = findConfigField(configs, paramTypeRaw);
            bw.write("        entitySave(" + ifaceName + "." + configField + ", " + method.params.get(0).name + ");\n");
        } else if (name.startsWith("delete")) {
            if (method.params.size() == 1) {
                String paramTypeRaw = toRawTypeString(method.params.get(0).type);
                String configField = findConfigField(configs, paramTypeRaw);
                bw.write("        entityDelete(" + ifaceName + "." + configField + ", " + method.params.get(0).name
                        + ");\n");
            }
        }
    }

    // --- Return type helpers ---

    enum ReturnKind {
        VOID, INT, ONE, OPTIONAL, LIST
    }

    private ReturnKind resolveReturnKind(TypeMirror returnType) {
        if (returnType.getKind() == TypeKind.VOID)
            return ReturnKind.VOID;
        if (returnType.getKind() == TypeKind.INT)
            return ReturnKind.INT;
        if (returnType.getKind() == TypeKind.LONG)
            return ReturnKind.ONE;
        if (returnType.getKind() == TypeKind.DECLARED) {
            DeclaredType dt = (DeclaredType) returnType;
            String name = ((TypeElement) dt.asElement()).getQualifiedName().toString();
            if ("java.lang.Integer".equals(name))
                return ReturnKind.INT;
            if ("java.util.Optional".equals(name))
                return ReturnKind.OPTIONAL;
            if ("java.util.List".equals(name))
                return ReturnKind.LIST;
        }
        return ReturnKind.ONE;
    }

    /**
     * Returns [rawElementType, genericTypeExpression] for the element type.
     */
    private String[] resolveElementType(TypeMirror returnType) {
        if (returnType.getKind() == TypeKind.DECLARED) {
            DeclaredType dt = (DeclaredType) returnType;
            String name = ((TypeElement) dt.asElement()).getQualifiedName().toString();
            if (("java.util.Optional".equals(name) || "java.util.List".equals(name))
                    && !dt.getTypeArguments().isEmpty()) {
                TypeMirror arg = dt.getTypeArguments().get(0);
                String rawType = toRawTypeString(arg);
                String genericExpr = toGenericTypeExpression(arg);
                return new String[] { rawType, genericExpr };
            }
        }
        if (returnType.getKind() == TypeKind.LONG) {
            return new String[] { "long", "long.class" };
        }
        String raw = toRawTypeString(returnType);
        return new String[] { raw, raw + ".class" };
    }

    private String toGenericTypeExpression(TypeMirror type) {
        if (type.getKind().isPrimitive())
            return type.toString() + ".class";
        if (type.getKind() == TypeKind.DECLARED) {
            DeclaredType dt = (DeclaredType) type;
            String rawName = ((TypeElement) dt.asElement()).getQualifiedName().toString();
            if (dt.getTypeArguments().isEmpty())
                return rawName + ".class";
            var sb = new StringBuilder();
            sb.append("new eu.aston.beanmeta.ParameterizedTypeImpl(").append(rawName).append(".class, new Type[]{");
            for (int i = 0; i < dt.getTypeArguments().size(); i++) {
                if (i > 0)
                    sb.append(", ");
                sb.append(toGenericTypeExpression(dt.getTypeArguments().get(i)));
            }
            sb.append("})");
            return sb.toString();
        }
        return type.toString() + ".class";
    }

    // --- Entity config matching ---

    private String findConfigField(List<EntityConfigInfo> configs, String entityType) {
        for (EntityConfigInfo c : configs) {
            if (c.entityType.equals(entityType))
                return c.fieldName;
        }
        throw new RuntimeException("No EntityConfig found for type: " + entityType);
    }

    // --- String helpers ---

    private String toTypeString(TypeMirror type) {
        return type.toString();
    }

    private String toRawTypeString(TypeMirror type) {
        if (type.getKind().isPrimitive())
            return type.toString();
        if (type.getKind() == TypeKind.DECLARED) {
            DeclaredType dt = (DeclaredType) type;
            return ((TypeElement) dt.asElement()).getQualifiedName().toString();
        }
        return type.toString();
    }

    /** Generate QueryParam map from bean properties via BeanMetaRegistry (runtime resolution). */
    private String buildBeanQueryParamMapExpr(ParamInfo beanParam) {
        String beanType = toRawTypeString(beanParam.type);
        return "eu.aston.dao.impl.QueryParam.fromBeanMeta(eu.aston.beanmeta.BeanMetaRegistry.forClass(" + beanType
                + ".class))";
    }

    /** Generate Map.of("name", new QueryParam("name", 0, Type.class), ...) expression. */
    private String buildQueryParamMapExpr(List<ParamInfo> params) {
        var sb = new StringBuilder("java.util.Map.of(");
        for (int i = 0; i < params.size(); i++) {
            if (i > 0)
                sb.append(", ");
            String name = params.get(i).name;
            String typeName = toRawTypeString(params.get(i).type);
            sb.append("\"").append(name).append("\", ");
            sb.append("new eu.aston.dao.impl.QueryParam(\"").append(name).append("\", ").append(i).append(", ")
                    .append(typeName).append(".class)");
        }
        sb.append(")");
        return sb.toString();
    }

    private String joinParamValues(List<ParamInfo> params) {
        var sb = new StringBuilder();
        for (int i = 0; i < params.size(); i++) {
            if (i > 0)
                sb.append(", ");
            sb.append(params.get(i).name);
        }
        return sb.toString();
    }

    private static final Set<String> SCALAR_TYPE_NAMES = Set.of("java.lang.String", "java.lang.Boolean", "boolean",
            "java.lang.Integer", "int", "java.lang.Long", "long", "java.lang.Short", "short", "java.lang.Byte", "byte",
            "java.lang.Float", "float", "java.lang.Double", "double", "java.math.BigDecimal", "java.time.Instant",
            "java.time.LocalDate", "java.time.LocalDateTime", "java.util.UUID", "byte[]", "eu.aston.dao.Spread",
            "eu.aston.dao.ICondition");

    private boolean isBeanType(TypeMirror type) {
        if (type.getKind().isPrimitive())
            return false;
        if (type.getKind() != TypeKind.DECLARED)
            return false;
        String name = ((TypeElement) ((DeclaredType) type).asElement()).getQualifiedName().toString();
        return !SCALAR_TYPE_NAMES.contains(name);
    }

    private String escapeJava(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t",
                "\\t");
    }

    private String getPackageName(TypeElement typeElement) {
        Element enclosing = typeElement.getEnclosingElement();
        if (enclosing instanceof PackageElement pkg) {
            return pkg.getQualifiedName().toString();
        }
        return "";
    }

    private void writeServiceFile() {
        try {
            FileObject resource = processingEnv.getFiler().createResource(StandardLocation.CLASS_OUTPUT, "",
                    "META-INF/services/eu.aston.dao.DaoProvider");
            try (Writer writer = resource.openWriter()) {
                for (String entry : generatedServiceEntries) {
                    writer.write(entry + "\n");
                }
            }
        } catch (IOException e) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "Failed to write service file: " + e.getMessage());
        }
    }

    // --- Inner records ---

    private record EntityConfigInfo(String fieldName, String entityType) {
    }

    private record ParamInfo(String name, TypeMirror type) {
    }

    private record MethodInfo(String name, TypeMirror returnType, TypeMirror genericReturnType, List<ParamInfo> params,
            String queryValue, ExecutableElement element) {
        TypeMirror getGenericReturnType() {
            return element.getReturnType();
        }
    }
}
