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

(defun alist-get (key alist)
  (cdr (assoc key alist)))

(defun alist-get-str (str-key alist)
  (cdr (assoc str-key alist :test #'equal)))

(defun alist-set (key value alist &key (test #'eql))
  (let ((existing (assoc key alist :test test)))
    (if existing
        (rplacd existing value)
        (rplacd (last alist) `((,key . ,value))))
    alist))

(defun alist-update (al1 al2 &key (test #'eql))
  (dolist (kv al2)
    (alist-set (car kv) (cdr kv) al1 :test test))
  al1)

(defun alist-merge (alists &key (test #'eql))
  (reduce (lambda (a b) (alist-update a b :test test))
          (cdr alists)
          :initial-value (copy-alist (car alists))))
