#!/bin/bash
set -e
quicklisp=${QUICKLISP:-~/quicklisp}
mkdir -p dist

# prime the quicklisp cache and compile all the
# dependencies as a separate step
sbcl --eval '(require "nellie")' --eval '(quit)'

# actually build the thing
buildapp --output dist/nellie \
         --asdf-tree $quicklisp/dists/quicklisp/installed \
         --eval '(push #p"./" asdf:*central-registry*)' \
         --load-system nellie \
         --eval '(defun main (&args) (nellie.ui::main))' \
         --entry main

