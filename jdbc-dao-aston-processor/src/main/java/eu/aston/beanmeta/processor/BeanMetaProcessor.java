package eu.aston.beanmeta.processor;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.RecordComponentElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
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

@SupportedAnnotationTypes("eu.aston.beanmeta.GenerateMeta")
@SupportedSourceVersion(SourceVersion.RELEASE_17)
public class BeanMetaProcessor extends AbstractProcessor {

    private final Set<String> generatedServiceEntries = new LinkedHashSet<>();

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (TypeElement annotation : annotations) {
            for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
                if (element instanceof TypeElement typeElement) {
                    try {
                        processType(typeElement);
                    } catch (IOException e) {
                        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                                "Failed to generate BeanMeta: " + e.getMessage(), element);
                    }
                }
            }
        }

        if (roundEnv.processingOver() && !generatedServiceEntries.isEmpty()) {
            writeServiceFile();
        }

        return true;
    }

    private void processType(TypeElement typeElement) throws IOException {
        boolean isRecord = typeElement.getKind() == ElementKind.RECORD;
        String packageName = getPackageName(typeElement);
        String simpleName = typeElement.getSimpleName().toString();
        String metaClassName = simpleName + "Meta";
        String qualifiedMetaName = packageName.isEmpty() ? metaClassName : packageName + "." + metaClassName;
        String qualifiedTypeName = typeElement.getQualifiedName().toString();

        List<PropertyInfo> properties;
        if (isRecord) {
            properties = extractRecordProperties(typeElement);
        } else {
            properties = extractBeanProperties(typeElement);
        }

        Filer filer = processingEnv.getFiler();
        JavaFileObject sourceFile = filer.createSourceFile(qualifiedMetaName, typeElement);

        try (Writer writer = sourceFile.openWriter();
             BufferedWriter bw = new BufferedWriter(writer)) {
            generateSource(bw, packageName, simpleName, metaClassName, qualifiedTypeName, properties, isRecord);
        }

        generatedServiceEntries.add(qualifiedMetaName);
    }

    private List<PropertyInfo> extractRecordProperties(TypeElement typeElement) {
        var properties = new ArrayList<PropertyInfo>();
        for (RecordComponentElement rc : typeElement.getRecordComponents()) {
            properties.add(new PropertyInfo(
                    rc.getSimpleName().toString(),
                    rc.asType(),
                    rc.getSimpleName().toString() // accessor name = component name for records
            ));
        }
        return properties;
    }

    private List<PropertyInfo> extractBeanProperties(TypeElement typeElement) {
        var properties = new ArrayList<PropertyInfo>();
        for (Element enclosed : typeElement.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.METHOD) continue;
            ExecutableElement method = (ExecutableElement) enclosed;
            if (method.getModifiers().contains(Modifier.STATIC)) continue;
            if (!method.getParameters().isEmpty()) continue;

            String methodName = method.getSimpleName().toString();
            String propName = null;
            if (methodName.startsWith("get") && methodName.length() > 3) {
                propName = Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4);
            } else if (methodName.startsWith("is") && methodName.length() > 2) {
                TypeMirror returnType = method.getReturnType();
                if (returnType.getKind() == TypeKind.BOOLEAN) {
                    propName = Character.toLowerCase(methodName.charAt(2)) + methodName.substring(3);
                }
            }

            if (propName != null && !"class".equals(propName)) {
                properties.add(new PropertyInfo(propName, method.getReturnType(), methodName));
            }
        }
        // Also scan superclass methods
        TypeMirror superclass = typeElement.getSuperclass();
        if (superclass.getKind() == TypeKind.DECLARED) {
            TypeElement superElement = (TypeElement) ((DeclaredType) superclass).asElement();
            if (!superElement.getQualifiedName().toString().equals("java.lang.Object")) {
                for (Element enclosed : superElement.getEnclosedElements()) {
                    if (enclosed.getKind() != ElementKind.METHOD) continue;
                    ExecutableElement method = (ExecutableElement) enclosed;
                    if (method.getModifiers().contains(Modifier.STATIC)) continue;
                    if (!method.getModifiers().contains(Modifier.PUBLIC)) continue;
                    if (!method.getParameters().isEmpty()) continue;

                    String methodName = method.getSimpleName().toString();
                    String propName = null;
                    if (methodName.startsWith("get") && methodName.length() > 3) {
                        propName = Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4);
                    } else if (methodName.startsWith("is") && methodName.length() > 2) {
                        if (method.getReturnType().getKind() == TypeKind.BOOLEAN) {
                            propName = Character.toLowerCase(methodName.charAt(2)) + methodName.substring(3);
                        }
                    }

                    if (propName != null && !"class".equals(propName)) {
                        // only add if not already present
                        String finalPropName = propName;
                        boolean alreadyPresent = properties.stream()
                                .anyMatch(p -> p.name.equals(finalPropName));
                        if (!alreadyPresent) {
                            properties.add(new PropertyInfo(propName, method.getReturnType(), methodName));
                        }
                    }
                }
            }
        }
        return properties;
    }

    private void generateSource(BufferedWriter bw, String packageName, String simpleName,
                                String metaClassName, String qualifiedTypeName,
                                List<PropertyInfo> properties, boolean isRecord) throws IOException {
        if (!packageName.isEmpty()) {
            bw.write("package " + packageName + ";\n\n");
        }

        bw.write("import eu.aston.beanmeta.BeanMeta;\n");
        bw.write("import eu.aston.beanmeta.ParameterizedTypeImpl;\n");
        bw.write("import java.lang.reflect.Type;\n");
        bw.write("import java.util.List;\n\n");

        bw.write("public final class " + metaClassName + " implements BeanMeta<" + simpleName + "> {\n\n");

        // type()
        bw.write("    @Override\n");
        bw.write("    public Class<" + simpleName + "> type() {\n");
        bw.write("        return " + simpleName + ".class;\n");
        bw.write("    }\n\n");

        // names()
        bw.write("    @Override\n");
        bw.write("    public List<String> names() {\n");
        bw.write("        return List.of(" + joinQuoted(properties) + ");\n");
        bw.write("    }\n\n");

        // types()
        bw.write("    @Override\n");
        bw.write("    public List<Class<?>> types() {\n");
        bw.write("        return List.of(" + joinRawTypes(properties) + ");\n");
        bw.write("    }\n\n");

        // genericTypes()
        bw.write("    @Override\n");
        bw.write("    public List<Type> genericTypes() {\n");
        bw.write("        return List.of(\n");
        bw.write("            " + joinGenericTypes(properties) + "\n");
        bw.write("        );\n");
        bw.write("    }\n\n");

        // get()
        bw.write("    @Override\n");
        bw.write("    public Object get(" + simpleName + " instance, String name) {\n");
        bw.write("        return switch (name) {\n");
        for (PropertyInfo prop : properties) {
            bw.write("            case \"" + prop.name + "\" -> instance." + prop.accessorName + "();\n");
        }
        bw.write("            default -> throw new IllegalArgumentException(\"Unknown property: \" + name);\n");
        bw.write("        };\n");
        bw.write("    }\n\n");

        // create()
        bw.write("    @Override\n");
        bw.write("    @SuppressWarnings(\"unchecked\")\n");
        bw.write("    public " + simpleName + " create(Object... values) {\n");
        if (isRecord) {
            bw.write("        return new " + simpleName + "(\n");
            for (int i = 0; i < properties.size(); i++) {
                PropertyInfo prop = properties.get(i);
                String cast = getCastExpression(prop.type, "values[" + i + "]");
                bw.write("            " + cast);
                if (i < properties.size() - 1) bw.write(",");
                bw.write("\n");
            }
            bw.write("        );\n");
        } else {
            // Bean: try setter-based or all-args constructor
            // For generated code, use all-args constructor approach
            bw.write("        return new " + simpleName + "(\n");
            for (int i = 0; i < properties.size(); i++) {
                PropertyInfo prop = properties.get(i);
                String cast = getCastExpression(prop.type, "values[" + i + "]");
                bw.write("            " + cast);
                if (i < properties.size() - 1) bw.write(",");
                bw.write("\n");
            }
            bw.write("        );\n");
        }
        bw.write("    }\n");

        bw.write("}\n");
    }

    private String joinQuoted(List<PropertyInfo> properties) {
        var sb = new StringBuilder();
        for (int i = 0; i < properties.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"").append(properties.get(i).name).append("\"");
        }
        return sb.toString();
    }

    private String joinRawTypes(List<PropertyInfo> properties) {
        var sb = new StringBuilder();
        for (int i = 0; i < properties.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(toRawTypeString(properties.get(i).type)).append(".class");
        }
        return sb.toString();
    }

    private String joinGenericTypes(List<PropertyInfo> properties) {
        var sb = new StringBuilder();
        for (int i = 0; i < properties.size(); i++) {
            if (i > 0) sb.append(",\n            ");
            sb.append(toGenericTypeExpression(properties.get(i).type));
        }
        return sb.toString();
    }

    private String toRawTypeString(TypeMirror type) {
        if (type.getKind().isPrimitive()) {
            return type.toString();
        }
        if (type.getKind() == TypeKind.DECLARED) {
            DeclaredType dt = (DeclaredType) type;
            return ((TypeElement) dt.asElement()).getQualifiedName().toString();
        }
        return type.toString();
    }

    private String toGenericTypeExpression(TypeMirror type) {
        if (type.getKind().isPrimitive()) {
            return type.toString() + ".class";
        }
        if (type.getKind() == TypeKind.DECLARED) {
            DeclaredType dt = (DeclaredType) type;
            String rawName = ((TypeElement) dt.asElement()).getQualifiedName().toString();
            List<? extends TypeMirror> typeArgs = dt.getTypeArguments();
            if (typeArgs.isEmpty()) {
                return rawName + ".class";
            }
            var sb = new StringBuilder();
            sb.append("new ParameterizedTypeImpl(").append(rawName).append(".class, new Type[]{");
            for (int i = 0; i < typeArgs.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(toGenericTypeExpression(typeArgs.get(i)));
            }
            sb.append("})");
            return sb.toString();
        }
        return type.toString() + ".class";
    }

    private String getCastExpression(TypeMirror type, String valueExpr) {
        if (type.getKind().isPrimitive()) {
            String boxed = getBoxedName(type.getKind());
            return "(" + boxed + ") " + valueExpr;
        }
        if (type.getKind() == TypeKind.DECLARED) {
            DeclaredType dt = (DeclaredType) type;
            String rawName = ((TypeElement) dt.asElement()).getQualifiedName().toString();
            List<? extends TypeMirror> typeArgs = dt.getTypeArguments();
            if (typeArgs.isEmpty()) {
                return "(" + rawName + ") " + valueExpr;
            }
            // Use raw cast for parameterized types
            return "(" + rawName + ") " + valueExpr;
        }
        return "(" + type + ") " + valueExpr;
    }

    private String getBoxedName(TypeKind kind) {
        return switch (kind) {
            case BOOLEAN -> "Boolean";
            case BYTE -> "Byte";
            case SHORT -> "Short";
            case INT -> "Integer";
            case LONG -> "Long";
            case FLOAT -> "Float";
            case DOUBLE -> "Double";
            case CHAR -> "Character";
            default -> throw new IllegalArgumentException("Not a primitive: " + kind);
        };
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
            FileObject resource = processingEnv.getFiler().createResource(
                    StandardLocation.CLASS_OUTPUT, "",
                    "META-INF/services/eu.aston.beanmeta.BeanMeta");
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

    private record PropertyInfo(String name, TypeMirror type, String accessorName) {}
}
