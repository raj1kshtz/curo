package curo

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/doc"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const (
	apiGolden = "testdata/api.golden"

	apiHeader = `// Curo's exported API as go/doc sees it, without comments. It only changes
// when an exported declaration is added, removed, or changed. Review the diff
// against ADR-0010, then regenerate it with: go test -run '^TestAPI$' -update .

package curo
`
)

// TestAPI compares the exported API with testdata/api.golden, so that every
// change to a compatibility promise shows up in review.
func TestAPI(t *testing.T) {
	t.Parallel()

	got, err := renderAPI(".")
	if err != nil {
		t.Fatalf("renderAPI() error = %v", err)
	}
	if *updateGolden {
		err = os.MkdirAll(filepath.Dir(apiGolden), 0o750)
		if err != nil {
			t.Fatalf("MkdirAll() error = %v", err)
		}
		err = os.WriteFile(apiGolden, []byte(got), 0o600)
		if err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		return
	}

	data, err := os.ReadFile(apiGolden)
	if err != nil {
		t.Fatalf("ReadFile() error = %v; run go test -run '^TestAPI$' -update .", err)
	}
	want := strings.ReplaceAll(string(data), "\r\n", "\n")
	if line, wantLine, gotLine, ok := simFirstDifference(want, got); ok {
		t.Fatalf("exported API differs from %s at line %d:\nwant: %s\ngot:  %s\n"+
			"Review the change against ADR-0010, then run go test -run '^TestAPI$' -update .",
			apiGolden, line, wantLine, gotLine)
	}
}

// renderAPI prints the exported declarations of the package in dir as
// formatted Go source, in go/doc order: constants, variables, and functions,
// then each type followed by its constants, variables, functions, and
// methods. Comments, blank lines inside declarations, function bodies,
// receiver names, and unexported fields are left out, so documentation edits
// do not change the result. It refuses packages with cgo files or with
// non-test files that build constraints exclude, because their API would
// depend on the build configuration.
func renderAPI(dir string) (string, error) {
	pkg, err := build.ImportDir(dir, 0)
	if err != nil {
		return "", err
	}
	if len(pkg.CgoFiles) > 0 {
		return "", fmt.Errorf("cgo files %v are not supported", pkg.CgoFiles)
	}
	for _, name := range pkg.IgnoredGoFiles {
		if !strings.HasSuffix(name, "_test.go") {
			return "", fmt.Errorf("build constraints exclude %s", name)
		}
	}

	fset := token.NewFileSet()
	files := make([]*ast.File, 0, len(pkg.GoFiles))
	for _, name := range pkg.GoFiles {
		// Go 1.23's go/doc needs resolved objects, so keep object resolution.
		file, parseErr := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if parseErr != nil {
			return "", parseErr
		}
		files = append(files, file)
	}
	docs, err := doc.NewFromFiles(fset, files, "github.com/raj1kshtz/curo")
	if err != nil {
		return "", err
	}

	var (
		decls    []string
		printErr error
	)
	emit := func(node ast.Node) {
		var text strings.Builder
		if fprintErr := printer.Fprint(&text, fset, node); fprintErr != nil && printErr == nil {
			printErr = fprintErr
		}
		lines := slices.DeleteFunc(strings.Split(text.String(), "\n"), func(line string) bool {
			return strings.TrimSpace(line) == ""
		})
		decls = append(decls, strings.Join(lines, "\n"))
	}
	emitValues := func(values []*doc.Value) {
		for _, value := range values {
			emit(value.Decl)
		}
	}
	emitFuncs := func(funcs []*doc.Func) {
		for _, fn := range funcs {
			if fn.Decl.Recv != nil {
				for _, receiver := range fn.Decl.Recv.List {
					receiver.Names = nil
				}
			}
			emit(fn.Decl)
		}
	}

	emitValues(docs.Consts)
	emitValues(docs.Vars)
	emitFuncs(docs.Funcs)
	for _, typ := range docs.Types {
		emit(typ.Decl)
		emitValues(typ.Consts)
		emitValues(typ.Vars)
		emitFuncs(typ.Funcs)
		emitFuncs(typ.Methods)
	}
	if printErr != nil {
		return "", printErr
	}

	source, err := format.Source([]byte(apiHeader + "\n" + strings.Join(decls, "\n\n") + "\n"))
	if err != nil {
		return "", err
	}

	return string(source), nil
}
