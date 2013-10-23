(in-package :cl-user)

(defpackage :nellie.asd
  (:use :cl :asdf))

(in-package :nellie.asd)

(asdf:defsystem #:nellie
  :serial t
  :description "Away from the circus"
  :version "0.1"
  :author "Mike Ivanov"
  :license "LGPLv3"
  :depends-on (:iterate :drakma :flexi-streams :cl-ppcre :cl-json
               :getopt :iterate :simple-date-time :alexandria :cl-interpol)
  :components ((:file "alists")
               (:file "hdfs")
               (:file "ui")))
