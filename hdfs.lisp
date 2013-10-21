(in-package :cl-user)
(defpackage :nellie.hdfs
  (:nicknames #:hdfs)
  (:use :cl :alists :simple-date-time :iterate :alexandria)
  (:export #:+webfs-port+
           #:+webhdfs-port+
           #:*default-port*
           #:*namenode-rpc-address*
           #:make-context
           #:create
           #:append-to
           #:mkdirs
           #:list-status
           #:get-file-status
           #:delete-file-or-dir
           #:from-hadoop-time
           #:server-error
           #:server-error-code
           #:server-error-message))

(in-package :nellie.hdfs)

; http://hadoop.apache.org/docs/r1.2.1/webhdfs.html

(define-constant +webfs-port+ 14000)
(define-constant +webhdfs-port+ 50075)

(defparameter *default-port* +webfs-port+)
(defparameter *namenode-rpc-address* "localhost:8020")

(defstruct context
  (host "127.0.0.1")
  (port *default-port*)
  (user "hdfs"))

(define-condition server-error (error)
  ((type :initarg :type :reader server-error-type)
   (code :initarg :code :reader server-error-code)
   (message :initarg :message :reader server-error-message)))

(defmethod print-object ((object server-error) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (format stream "[~D ~A]: ~A"
            (server-error-type object)
            (server-error-code object)
            (server-error-message object))))

(defun server-error (type code message)
  (error 'server-error
         :type type
         :code code
         :message message))

(defun exception-to-keyword (exception)
  (make-keyword (cl-json::simplified-camel-case-to-lisp exception)))

(defun url-with-parameters (url parameters)
  (let* ((uri (puri:parse-uri url))
         (query-params (drakma::dissect-query (puri::uri-query uri)))
         (all-params (append query-params parameters)))
    (setf (puri:uri-query uri)
          (drakma::alist-to-url-encoded-string all-params
                                               drakma:*drakma-default-external-format*
                                               #'drakma:url-encode))
    uri))

(defun send-request (method url
                     &key parameters content (content-length t))
  (let ((drakma:*header-stream* nil))
    (multiple-value-bind (bytes code headers)
        (drakma:http-request (url-with-parameters url parameters)
                             :external-format-in :utf-8
                             :external-format-out :utf-8
                             :force-binary t
                             :method method
                             :redirect nil
                             :content content
                             :content-length content-length
                             :content-type "application/octet-stream")
      (let* ((content-type (alist-get :content-type headers))
             (is-json (equal content-type "application/json"))
             (body (cond (is-json
                          (when bytes
                            (json:decode-json-from-string (flexi-streams:octets-to-string bytes))))
                         ((equal (subseq content-type 0 5) "text/")
                          (flexi-streams:octets-to-string bytes))
                         (t bytes))))
        (cond ((< code 400) (values body code headers))
              (is-json (let* ((exception (alist-get :*remote-exception body))
                              (name (alist-get :exception exception))
                              (type (if name (exception-to-keyword name) :unknown-error))
                              (message (or (alist-get :message exception) "Unknown error")))
                         (server-error type code message)))
              (t (server-error :server-error code body)))))))

(defun operation-request (context method operation path &key parameters)
  (let* ((user (context-user context))
         (path (if (eq (char path 0) #\/) path
                   (format nil "/users/~A/~A" user path)))
         (url (format nil "http://~A:~D/webhdfs/v1~A"
                      (context-host context)
                      (context-port context)
                      path))
         (parameters (concatenate 'list
                                  `(("user.name" . ,user)
                                    ("op" . ,operation)
                                    ("namenoderpcaddress" . ,*namenode-rpc-address*))
                                  parameters)))
    (send-request method url :parameters parameters)))

(defun from-hadoop-time (hadoop-time)
  (let ((secs (floor (/ hadoop-time 1000.0))))
    (from-posix-time secs)))

(defun send-content (content callback stream)
  (unless (and (streamp content)
               (input-stream-p content)
               (open-stream-p content)
               (subtypep (stream-element-type content) 'flexi-streams:octet))
    (error "Expected a binary input stream"))
  (iter (with buf = (make-array 8192 :element-type 'flexi-streams:octet))
        (for num = (read-sequence buf content))
        (sum num into pos)
        (when callback (for dat first nil then (funcall callback pos dat)))
        (if (zerop num)
            (terminate)
            (write-sequence buf stream :end num))))

(defun create (context path content length
               &key overwrite blocksize replication permission buffersize callback)
  (let* ((parameters `(("overwrite" . ,(if overwrite "true" "false"))
                       ("blocksize" . ,blocksize)
                       ("replication" . ,replication)
                       ("permission" . ,permission)
                       ("buffersize" . ,buffersize)))
         (headers1 (nth-value 2 (operation-request context :put "CREATE" path
                                                   :parameters parameters)))
         (location1 (alist-get :location headers1))
         (headers2 (nth-value 2 (send-request :put location1
                                              :content (curry #'send-content content callback)
                                              :content-length length)))
         (location2 (or (alist-get :location headers2) path)))
    location2))

(defun append-to (context path content length &key buffersize callback)
  (let* ((parameters `(("buffersize" . ,buffersize)))
         (headers1 (nth-value 2 (operation-request context :post "APPEND" path
                                                   :parameters parameters)))
         (location1 (alist-get :location headers1))
         (headers2 (nth-value 2 (send-request :post location1
                                              :content (curry #'send-content content callback)
                                              :content-length length)))
         (location2 (or (alist-get :location headers2) path)))
    location2))

(defun list-status (context path)
  (let* ((res (operation-request context :get "LISTSTATUS" path))
         (statuses (alist-get :*file-status (alist-get :*file-statuses res))))
    (iter (for status in statuses)
          (collect
              (let ((mtime (alist-get :modification-time status))
                    (atime (alist-get :access-time status))
                    (type  (alist-get :type status)))
                (alist-merge `(,status
                               ((:modification-time . ,(from-hadoop-time mtime))
                                (:access-time . ,(from-hadoop-time atime))
                                (:type . ,(make-keyword type))))))))))

(defun get-file-status (context path)
  (let ((res (operation-request context :get "GETFILESTATUS" path)))
    (alist-get :*file-status res)))

(defun mkdirs (context path &key permission)
  (let ((res (operation-request context :put "MKDIRS" path
                                :parameters `(("permission" . ,permission)))))
    (alist-get :boolean res)))

(defun delete-file-or-dir (context path &key recursive)
  (let ((res (operation-request context :delete "DELETE" path
                                :parameters `(("recursive" . ,(if recursive "true" "false"))))))
    (alist-get :boolean res)))
