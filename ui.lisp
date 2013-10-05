(in-package :cl-user)
(defpackage :nellie.ui
  (:use :cl :iterate :alists :simple-date-time :alexandria)
  (:export #:main))

(in-package :nellie.ui)

(define-condition ui-error (error)
  ((message :initarg :message :reader ui-error-message)))

(defmethod print-object ((object ui-error) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (format stream "~A" (ui-error-message object))))

(defun ui-error (message)
  (error 'ui-error :message message))

(defun format-date-time (dt)
  (format nil "~04,'0d-~02,'0d-~02,'0d ~02,'0d:~02,'0d:~02,'0d"
          (year-of dt) (month-of dt) (day-of dt)
          (hour-of dt) (minute-of dt) (second-of dt)))

(defun cmd-ls (context path)
  (let* ((lst (hdfs:list-status context path)))
    (dolist (e lst)
      (format t "~A~T~A~T~A~T~D~T~A~T~A~%"
              (alist-get :permission e)
              (alist-get :owner e)
              (alist-get :group e)
              (alist-get :length e)
              (format-date-time (alist-get :modification-time e))
              (concatenate 'string
                           (alist-get :path-suffix e)
                           (if (eq (alist-get :type e) :directory) "/" ""))))))

(defun cmd-fstat (context path)
  (let* ((st (hdfs:get-file-status context path)))
    (format t "Path:        ~A~%" path)
    (format t "Type:        ~A~%" (alist-get :type st))
    (format t "Length:      ~D~%" (alist-get :length st))
    (format t "Permission:  ~A~%" (alist-get :permission st))
    (format t "Owner:       ~A~%" (alist-get :owner st))
    (format t "Group:       ~A~%" (alist-get :group st))
    (format t "Mod. time:   ~A~%" (format-date-time (hdfs:from-hadoop-time
                                                     (alist-get :modification-time st))))
    (format t "Access time: ~A~%" (format-date-time (hdfs:from-hadoop-time
                                                     (max (alist-get :access-time st)
                                                          (alist-get :modification-time st)))))
    (format t "Block size:  ~D~%" (alist-get :block-size st))
    (format t "Replication: ~D~%" (alist-get :replication st))))

(defun report-progress (total current data)
  (let* ((progress (ceiling (* 10 (/ current total))))
         (reported (or data 0))
         (diff (- progress reported)))
    (if (plusp diff)
        (progn
          (format t ".")
          progress)
        reported)))

(defun cmd-put (context filename path)
  (with-open-file (in filename :element-type 'flexi-streams:octet)
    (let ((len (file-length in)))
      (format t "Uploading '~A' " filename)
      (hdfs:create context path in (file-length in)
                          :callback (curry #'report-progress len)
                          :overwrite t)
      (format t "~%Done.~%"))))

(defun cmd-mkdir (context path)
  (hdfs:mkdirs context path))

(defun cmd-rm (context path &key recursive)
  (hdfs:delete-file-or-dir context path :recursive recursive))

(defparameter +commands+ '(("ls"     . cmd-ls)
                           ("status" . cmd-status)
                           ("put"    . cmd-put)
                           ("fstat"  . cmd-fstat)
                           ("mkdir"  . cmd-mkdir)
                           ("rm"     . cmd-rm)))

(defparameter +options+ '(("host" :required)
                          ("port" :required)
                          ("user" :required)))

(defun exec-command (command args options)
  (let ((context (apply #'hdfs:make-context (flatten options))))
    (apply command (cons context args))))

(defun usage ()
  (format t "Usage: nellie [context options] <command> [command options]~%"))

(defun parse-options (argv)
  (multiple-value-bind (args options unrecognized)
      (getopt:getopt argv +options+)
    (let ((command (car args))
          (cmdargs (cdr args)))
      (cond ((null command) (ui-error "Please specify a command"))
            (unrecognized (ui-error (format nil "Unrecognized option `~A`"
                                            (car unrecognized))))
            (t (values command
                       cmdargs
                       (iter (for (k . v) in options)
                             (collect (cons (make-keyword (string-upcase k)) v)))))))))

(defun ui (&rest argv)
  (multiple-value-bind (command args options) (parse-options argv)
    (let ((handler (alist-get-str command +commands+)))
      (if (null handler)
          (ui-error (format nil "Unrecognized command `~S`" command))
          (exec-command handler args options)))))

(defun process-argv ()
  "list of tokens passed in at the cli"
  #+:sbcl (rest sb-ext:*posix-argv*)
  #+:ccl (rest ccl:*command-line-argument-list*)
  #+:clisp (rest ext:*args*)
  #+:lispworks (rest system:*line-arguments-list*)
  #+:cmu (rest extensions:*command-line-words*)
  #+:ecl (rest (ext:command-args))
  )

(defun main (&key (argv :none))
  (handler-case
      (let ((argv (if (eq argv :none) (process-argv) argv)))
        (apply #'ui argv))
    (ui-error (e)
      (format t "nellie: ~A~%" (ui-error-message e))
      (usage))
    (hdfs:server-error (e)
      (format t "nellie: ~A~%" (hdfs:server-error-message e)))
    (error (e)
      (format t "nellie: unexpected error (~A~%)" e))))
