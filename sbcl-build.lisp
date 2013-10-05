(in-package :cl)
(asdf:load-system :nellie)

(defun exec ()
  (nellie.ui:main)
  (sb-ext:exit))

(sb-ext:save-lisp-and-die "nellie" :toplevel #'exec :executable t)
