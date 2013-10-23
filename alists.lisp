(in-package :cl-user)
(defpackage :nellie.alists
  (:use :cl)
  (:nicknames #:alists)
  (:export #:alist-get
           #:alist-get-str
           #:alist-set
           #:alist-update
           #:alist-merge))

(in-package :nellie.alists)

(defun alist-get (alist key &key (test #'eql))
  (cdr (assoc key alist :test test)))

(defun alist-get-str (alist key-string)
  (cdr (assoc key-string alist :test #'equal)))

(defun alist-set (alist key value &key (test #'eql))
  (if (null alist)
      (list (cons key value))
      (let ((existing (assoc key alist :test test)))
        (if existing
            (rplacd existing value)
            (rplacd (last alist) (list (cons key value))))
        alist)))

(defun alist-update (alist updates-alist &key (test #'eql))
  (dolist (kv updates-alist)
    (setf alist (alist-set alist (car kv) (cdr kv) :test test)))
  alist)

(defun alist-merge (alists &key (test #'eql))
  (reduce (lambda (a b) (alist-update a b :test test)) alists))

