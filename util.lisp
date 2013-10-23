(in-package :cl-user)
(defpackage :nellie.util
  (:nicknames :util)
  (:use :cl :iterate :alexandria)
  (:import-from :cl-ppcre :scan-to-strings)
  (:import-from :metabang-bind :bind)
  (:export :alist-get
           :alist-get-str
           :alist-set
           :alist-update
           :alist-merge
           :string-to-keyword
           :symbol-to-keyword
           :alist-get :alist-get-str
           :with-re-match
           :with-accessors-in
           :terminate
           :getenv))

(in-package :nellie.util)

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

(defun string-to-keyword (string)
  (make-keyword (string-upcase string)))

(defun symbol-to-keyword (symbol)
  (string-to-keyword (symbol-name symbol)))

(defun with-re-match* (rx string callback)
  (when-let ((match (nth-value 1 (scan-to-strings rx string))))
    (apply callback (coerce match 'list))))

(defmacro with-re-match ((vars rx string) &body body)
  `(with-re-match* ,rx ,string (lambda (,@vars) ,@body)))

(defmacro with-accessors-in ((prefix slots object) &body body)
  `(bind (((:structure ,prefix ,@slots) ,object))
     ,@body))

(defun terminate-process (status)
  #+sbcl       (sb-ext:exit :code status)        ; SBCL
  #+ccl        (ccl:quit status)                 ; Clozure CL
  #+clisp      (ext:quit status)                 ; GNU CLISP
  #+cmu        (unix:unix-exit status)           ; CMUCL
  #+abcl       (ext:quit :status status)         ; Armed Bear CL
  #+allegro    (excl:exit status :quiet t)       ; Allegro CL
  #+lispworks  (lispworks:quit :status status)   ; LispWorks
  #+ecl        (ext:quit status)                 ; ECL
  )

(defun getenv (name &optional default)
    #+cmu
    (let ((x (assoc name ext:*environment-list*
                    :test #'string=)))
      (if x (cdr x) default))
    #-cmu
    (or
     #+allegro (sys:getenv name)
     #+clisp (ext:getenv name)
     #+ecl (si:getenv name)
     #+sbcl (sb-unix::posix-getenv name)
     #+ccl (ccl:getenv name)
     #+lispworks (lispworks:environment-variable name)
     default))
