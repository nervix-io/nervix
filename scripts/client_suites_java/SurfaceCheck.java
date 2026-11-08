import com.sun.source.tree.ClassTree;
import com.sun.source.tree.CompilationUnitTree;
import com.sun.source.tree.ImportTree;
import com.sun.source.tree.MemberSelectTree;
import com.sun.source.tree.MemberReferenceTree;
import com.sun.source.tree.IdentifierTree;
import com.sun.source.tree.MethodInvocationTree;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.NewClassTree;
import com.sun.source.util.JavacTask;
import com.sun.source.util.TreePathScanner;
import com.sun.source.util.Trees;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

/** Owns the JVM suite's declared surface, resolved references and stable JUnit identities.
 * Depends on Java 21's compiler trees/elements, the binding sources and JUnit declarations.
 * Must not load the native library, execute tests or emit compiled suite classes.
 */
public final class SurfaceCheck {
    private final Set<String> surface = new TreeSet<>();
    private final Set<String> references = new TreeSet<>();
    private final Set<String> tests = new TreeSet<>();
    private final Set<String> problems = new TreeSet<>();
    private final Trees trees;
    private final Elements elements;
    private final Path binding;
    private final Path suite;

    private SurfaceCheck(Trees trees, Elements elements, Path binding, Path suite) {
        this.trees = trees;
        this.elements = elements;
        this.binding = binding;
        this.suite = suite;
    }

    private static String identity(Element element) {
        if (element instanceof TypeElement type) return type.getQualifiedName().toString();
        if (element instanceof ExecutableElement method) {
            var parameters = new ArrayList<String>();
            for (var parameter : method.getParameters()) parameters.add(parameter.asType().toString());
            return identity(method.getEnclosingElement()) + "#" + method.getSimpleName()
                + "(" + String.join(",", parameters) + ")";
        }
        return identity(element.getEnclosingElement()) + "#" + element.getSimpleName();
    }

    private void declared(TypeElement type) {
        if (!type.getModifiers().contains(Modifier.PUBLIC)) return;
        surface.add(identity(type));
        for (var member : type.getEnclosedElements()) {
            if (member instanceof TypeElement nested) {
                declared(nested);
            } else if (member.getKind() == ElementKind.ENUM_CONSTANT) {
                surface.add(identity(member));
            } else if (member instanceof ExecutableElement method
                    && method.getModifiers().contains(Modifier.PUBLIC)
                    && (trees.getTree(method) != null || type.getKind() == ElementKind.RECORD)) {
                // Record components are public accessor methods even without a written body.
                // The Object methods javac synthesizes are not declarations of this API.
                boolean component = type.getRecordComponents().stream().anyMatch(value ->
                    value.getSimpleName().equals(method.getSimpleName()) && method.getParameters().isEmpty());
                if (trees.getTree(method) != null || component || method.getKind() == ElementKind.CONSTRUCTOR)
                    surface.add(identity(method));
            }
        }
    }

    private void referenced(Element element) {
        if (element == null) return;
        if (!(element instanceof TypeElement) && !(element instanceof ExecutableElement)
                && element.getKind() != ElementKind.ENUM_CONSTANT && element.getKind() != ElementKind.FIELD)
            return;
        String name = identity(element);
        if (surface.contains(name)) references.add(name);
        Element parent = element.getEnclosingElement();
        while (parent instanceof TypeElement) {
            String owner = identity(parent);
            if (surface.contains(owner)) references.add(owner);
            parent = parent.getEnclosingElement();
        }
    }

    private void scan(CompilationUnitTree unit) {
        Path source = Path.of(unit.getSourceFile().toUri()).toAbsolutePath().normalize();
        if (source.equals(binding)) {
            for (var declaration : unit.getTypeDecls()) {
                if (declaration instanceof ClassTree type) {
                    var element = trees.getElement(trees.getPath(unit, type));
                    if (element instanceof TypeElement declared) declared(declared);
                }
            }
        }
    }

    private void uses(CompilationUnitTree unit) {
        Path source = Path.of(unit.getSourceFile().toUri()).toAbsolutePath().normalize();
        if (!source.startsWith(suite)) return;
        new TreePathScanner<Void, Void>() {
            @Override public Void visitImport(ImportTree tree, Void unused) {
                return null; // An unused import is not an exercised API member.
            }
            @Override public Void visitIdentifier(IdentifierTree tree, Void unused) {
                referenced(trees.getElement(getCurrentPath()));
                return super.visitIdentifier(tree, unused);
            }
            @Override public Void visitMemberSelect(MemberSelectTree tree, Void unused) {
                referenced(trees.getElement(getCurrentPath()));
                return super.visitMemberSelect(tree, unused);
            }
            @Override public Void visitMethodInvocation(MethodInvocationTree tree, Void unused) {
                referenced(trees.getElement(getCurrentPath()));
                return super.visitMethodInvocation(tree, unused);
            }
            @Override public Void visitMemberReference(MemberReferenceTree tree, Void unused) {
                referenced(trees.getElement(getCurrentPath()));
                return super.visitMemberReference(tree, unused);
            }
            @Override public Void visitNewClass(NewClassTree tree, Void unused) {
                referenced(trees.getElement(getCurrentPath()));
                return super.visitNewClass(tree, unused);
            }
            @Override public Void visitMethod(MethodTree tree, Void unused) {
                var element = trees.getElement(getCurrentPath());
                if (element instanceof ExecutableElement method) {
                    boolean test = false;
                    for (var annotation : method.getAnnotationMirrors()) {
                        String annotationName = annotation.getAnnotationType().toString();
                        if (annotationName.equals("org.junit.jupiter.api.Test")) test = true;
                        if (annotationName.startsWith("org.junit.jupiter.params.")
                                || annotationName.equals("org.junit.jupiter.api.TestFactory")
                                || annotationName.equals("org.junit.jupiter.api.RepeatedTest"))
                            problems.add(source + ": suite tests need one stable, non-parameterized identity");
                    }
                    if (test) {
                        String name = elements.getBinaryName((TypeElement) method.getEnclosingElement())
                            + "#" + method.getSimpleName();
                        String tier = suite.relativize(source).getName(0).toString();
                        if (!Set.of("boundary", "cluster").contains(tier))
                            problems.add(name + " is outside the boundary and cluster tiers");
                        else if (!method.getParameters().isEmpty())
                            problems.add(name + " has parameters; suite tests have stable no-argument identities");
                        else tests.add(tier + "\t" + name);
                    }
                }
                return super.visitMethod(tree, unused);
            }
        }.scan(unit, null);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("expected binding.java suite classpath");
        Path binding = Path.of(args[0]).toAbsolutePath().normalize();
        Path suite = Path.of(args[1]).toAbsolutePath().normalize();
        var sources = new ArrayList<Path>();
        try (var files = Files.walk(binding.getParent())) {
            files.filter(path -> path.toString().endsWith(".java")).sorted().forEach(sources::add);
        }
        try (var files = Files.walk(suite)) {
            files.filter(path -> path.toString().endsWith(".java")).sorted().forEach(sources::add);
        }
        var compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) throw new IllegalStateException("Java 21 JDK is required for the JVM surface check");
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (var manager = compiler.getStandardFileManager(diagnostics, null, null)) {
            var inputs = manager.getJavaFileObjectsFromPaths(sources);
            var task = (JavacTask) compiler.getTask(null, manager, diagnostics,
                List.of("--release", "21", "-proc:none", "-classpath", args[2]), null, inputs);
            var units = new ArrayList<CompilationUnitTree>();
            task.parse().forEach(units::add);
            task.analyze();
            boolean failed = false;
            for (var diagnostic : diagnostics.getDiagnostics()) {
                if (diagnostic.getKind() == Diagnostic.Kind.ERROR) {
                    System.err.println(diagnostic);
                    failed = true;
                }
            }
            if (failed) System.exit(1);
            var check = new SurfaceCheck(Trees.instance(task), task.getElements(), binding, suite);
            units.forEach(check::scan);
            units.forEach(check::uses);
            check.surface.forEach(value -> System.out.println("SURFACE\t" + value));
            check.references.forEach(value -> System.out.println("REFERENCE\t" + value));
            check.tests.forEach(value -> System.out.println("TEST\t" + value));
            check.problems.forEach(value -> System.out.println("PROBLEM\t" + value));
        }
    }
}
