;;
;; dc-tiffany.lisp
;;
;; Where this program has not run before, you'll need to obtain these
;; packages and ensure that asdf can find them:
;;     :dc-utilities
;;     :ht-routes
;;     :dc-db
;; You'll also need to ql:quickload some stuff, like this:
;;     (ql:quickload '(:cl-ppcre :cl-markup :parenscript :hunchentoot
;;                     :postmodern :ironclad :yason :lass :dc-urlencode))
;;
;; The program needs the following database:
;;     pg_restore -U chattermancy -h localhost -Cd wikipedia wikipedia-1.pgdump
;;

(defpackage :dc-tiffany

  (:use :cl
        :cl-ppcre
        :cl-markup
        :lass
        :parenscript
        :dc-utilities
        :hunchentoot
        :do-urlencode
        :ht-routes
        :dc-db
        :postmodern
        :sb-thread
        :swank))

(in-package :dc-tiffany)

(defun home-based (path)
  (format nil "~a/~a" (namestring (user-homedir-pathname)) path))

(defparameter *protocol* "http")
(defparameter *host* "iqclone.com")
(defparameter *port* 4242)
(defparameter *contrib-dir* (home-based "lib/web-contrib"))
(defparameter *access-log* (home-based "log/tiffany/access.log"))
(defparameter *error-log* (home-based "log/tiffany/error.log"))
(defparameter *output* (home-based "log/tiffany/output.log"))
(defparameter *affinity-table* 'affinity_top100k)
(defparameter *articles-table* 'articles_top100k)
(defparameter *bootstrap* "/bootstrap/css/bootstrap.min.css")
(defparameter *bootstrap-theme* "/bootstrap/css/bootstrap-theme.min.css")
(defparameter *bootstrap-js* "/bootstrap/js/bootstrap.min.js")
(defparameter *ui-bootstrap* "/ui-bootstrap-tpls-0.12.0.min.js")
(defparameter *angularjs* "/angular.min.js")
(defparameter *jquery* "/contrib/jquery-1.11.2.min.js")
(defparameter *db*
  (ds `(:map
        :db "wikipedia"
        :username "chattermancy"
        :password "weasel1024"
        :host "localhost"
        :retry-count 1
        :retry-sleep 1
        :retry-sleep-factor 1
        :log-function ,(lambda (s) (format t "~a~%" s)))))
(defparameter *acceptor* nil)

;;
;; Support utilities
;;

(defun assemble-url (path-parts &key params)
  (let* ((base-url (format nil "~a://~a:~d/" *protocol* *host* *port*))
         (path-parts (if (and path-parts (atom path-parts))
                         (list path-parts)
                         path-parts))
         (path (if path-parts (apply #'join-paths
                                     (mapcar #'url-encode path-parts))
                   ""))
         (query-string (if params (format nil "?~{~a=~a~^&~}"
                                          (mapcar #'url-encode params))
                           "")))
    (format nil "~a~a~a" base-url path query-string)))

(defun set-content-type (type)
  (when (boundp '*reply*)
    (setf (hunchentoot:content-type*) type)))

(defun file-mime-type (file-name)
  (dc-mime-type (intern (string-upcase (file-extension file-name)) :keyword)))

(defun dc-mime-type (key)
  (case key
    (:js "text/javascript")
    (:json "application/json")
    (:jsonp "text/javascript")
    (:txt "text/plain")
    (:html "text/html")
    (:css "text/css")
    (:png "image/png")
    (:jpeg "image/jpeg")
    (:gif "image/gif")
    (:icon "image/x-icon")
    (:woff "application/x-font-woff")))

(defun binary-mime-type (mime-type)
  (when (member mime-type '("image/png"
                            "image/jpeg"
                            "image/gif"
                            "image/x-icon"
                            "application/x-font-woff")
                :test 'equal)
    t))

(defun web-server-start ()
  (if *acceptor*
      (format t "Web server is already running.~%")
      (let ((access-stream (open *access-log*
                                 :direction :output
                                 :if-exists :append
                                 :if-does-not-exist :create))
            (message-stream (open *error-log*
                                  :direction :output
                                  :if-exists :append
                                  :if-does-not-exist :create)))
        (setf *acceptor*
              (start (make-instance 'ht-routes:ht-acceptor
                                    :access-log-destination access-stream
                                    :message-log-destination message-stream
                                    :port *port*
                                    :persistent-connections-p nil)))
        (format t "Started web server.~%"))))

(defun web-server-stop ()
  (if *acceptor*
      (progn
        (stop *acceptor*)
        (setf *acceptor* nil)
        (format t "Stopped web server.~%"))
      (format t "Web server is not running.~%")))

(defun web-server-bounce ()
  (web-server-stop)
  (web-server-start))

(defun plists-to-json (result)
  (ds-to-json (ds (cons :array (mapcar (λ (row) (cons :map row)) result)))))

(defun to-log (control-string &rest variables)
  (with-open-file (log-stream *output* :direction :output
                              :if-exists :append
                              :if-does-not-exist :create)
    (apply #'format (append (list log-stream control-string) variables))))


;;
;; Web services
;;

(map-routes
 (post "/affinity/:wid" ws-post-affinity :wid "\\d+")
 (get "/" ws-home)
 (get "/styles" ws-styles)
 (get "/contrib/:file" ws-contrib :file ".+")
 (get "/app" ws-app)
 (get "/article-titles/:term" ws-article-titles :term ".+")
 (get "/most-similar/:title" ws-most-similar :title ".+"))

(defun ws-article-titles (params)
  (set-content-type (dc-mime-type :json))
  (plists-to-json
   (let ((term (url-decode (getf params :term))))
     (to-log "'~a'~%" term)
     (db-cmd 
      *db* :query
      (:limit
       (:order-by
        (:select 'wid 'title (:as (:similarity 'title term) 'simil)
                 :from *articles-table*
                 :where (:ilike 'title (format nil "%~a%" term)))
        (:desc 'simil))  ;; order by
       20)       ;; limit
       :plists))))

(defun ws-most-similar (params)
  (set-content-type (dc-mime-type :json))
  (plists-to-json
   (mapcar
    (lambda (x) (if (numberp x) (format nil "~,6f" x) x))
    (let ((title (url-decode (getf params :title))))
      (to-log "'~a'~%" title)
      (db-cmd
       *db* :query
       (:limit
        (:order-by
         (:select 'f.score
                  'a.title
                  (:as (:|| "http://en.wikipedia.org/wiki/"
                         (:replace 'a.title " " "_"))
                       'url)
          :from (:as *affinity-table* 'f)
          :inner-join (:as *articles-table* 'a)
          :on (:= 'f.bid 'a.wid)
          :where (:= 'f.aid (:select 'wid
                             :from *articles-table*
                             :where (:= 'title title))))
         (:desc 'f.score))
        15)
       :plists)))))

(defun ws-app ()
  (set-content-type (dc-mime-type :js))
  (ps (let ((app (angular.module "tiffany" ["ui.bootstrap"])))
        (app.controller
         "TypeaheadCtrl"
         (lambda ($scope $http)
           (setf $scope.selected undefined)

           (setf $scope.get-article
                 (lambda (val)
                   (chain
                    ($http.get (+ (lisp (assemble-url "article-titles"))
                                  "/" val))
                    (then
                     (lambda (response)
                       (response.data.map
                        (lambda (item)
                          item.title)))))))
 
           (setf $scope.get-most-similar
                 (lambda (ref)
                   (console.log (+ "ref=" ref))
                   (chain
                    ($http.get (+ (lisp (assemble-url "most-similar"))
                                  "/" ref))
                    (then
                     (lambda (response)
                       (setf $scope.most-similar-articles response.data)
                       (console.log $scope.most-similar-articles)))))))))))

(defun ws-styles ()
  (set-content-type (dc-mime-type :css))
  (compile-and-write
   '((:or .nav .pagination .carousel (.panel-title a)) :cursor "pointer")
   '(.main-content :margin "60px 10px 0 10px")
   '(.td-rank :width "76px")
   '(.td-score :width "120px")
   '(img :margin-top "5px")))

(defun ws-contrib (params)
  (let* ((file (getf params :file))
         (mime-type (file-mime-type file))
         (path (join-paths *contrib-dir* file))
         (data (if (binary-mime-type mime-type)
                   (slurp-binary path)
                   (slurp path))))
    (set-content-type mime-type)
    data))

(defun ws-home ()
  (to-log "hello world")
  (set-content-type (dc-mime-type :html))
  (markup
   (:html
    :lang "en"
    :ng-app "tiffany"
    (raw (page-head "Tiffany"))
    (:body
     (raw (body-head "/app"))
     (raw (body-nav))
     (:div :class "main-content" (raw (body-content)))))))

(defun ws-post-affinity (params)
  ;; /affinity/:wid
  (let ((reference-wid (getf params :wid))
        (affinity-array (thaw (raw-post-data :force-text t))))
    (loop for (wid score) across affinity-array
       do (insert-affinity reference-wid wid score))))

;;
;; Web service support functions
;;

(defun body-nav ()
  (markup
   (:nav :class "navbar navbar-inverse navbar-fixed-top"
         (:div :class "container"
               (:div :class "navbar-header"
                     (:button :type "button"
                              :class "navbar-toggle collapsed"
                              :data-toggle "collapse"
                              :data-target "#navbar"
                              :aria-expanded "false"
                              :aria-controls "navbar"
                              (:span :class "sr-only" "Toggle navigation")
                              (:span :class "icon-bar" nil)
                              (:span :class "icon-bar" nil)
                              (:span :class "icon-bar" nil))
                     (:a :class "navbar-brand" :href "#" "Tiffany"))
               (:div :id "navbar" :class "navbar-collapse collapse"
                     (:ul :class "nav navbar-nav"
                          (:li :class "active" (:a :href "#" "Home"))
                          (:li (:a :href "#about" "About"))
                          (:li (:a :href "#contact" "Contact"))))))))

(defun body-content ()
  (markup
   (:div :class "container-fluid"
         :ng-controller "TypeaheadCtrl"
         (:div :class "row"
               (:div :class "col-md-4"
                     (:div :class "row"
                           (raw (reference-wikipedia-article))))
               (:div :class "col-md-1" nil)
               (:div :class "col-md-7"
                     (raw (most-similar-wikipedia-articles)))))))

(defun reference-wikipedia-article ()
  (markup
   (:h4 "Reference Wikipedia Article")
   (:input :type "text" :ng-model "asyncSelected"
           :placeholder "Wikipedia article"
           :typeahead "article for article in getArticle($viewValue)"
           :typeahead-loading "loadingLocations"
           :typeahead-editable "false"
           :typeahead-on-select "getMostSimilar($item)"
           :class "form-control")
   (:img :ng-show "loadingLocations"
         :src "/contrib/loading.gif" nil)))

(defun reference-external-article ()
  (markup
   (:h4 "Reference External Article")))

(defun most-similar-wikipedia-articles ()
  (markup
   (:h4 "Most Similar Articles")
   (:table
    (:tr (:th :class "td-rank" "Rank")
         (:th :class "td-score" "Score")
         (:th "Title"))
    (:tr :ng-repeat "article in mostSimilarArticles"
         (:td "{{ $index + 1 }}")
         (:td "{{ article.score }}")
         (:td (:a :href "{{ article.url }}"
                  :target "_blank"
              "{{ article.title }}"))))))

(defun page-head (title)
  (markup
   (:head
    (:meta :charset "utf-8")
    (:meta :http-equiv "X-UA-Compatible" :content "IE=edge")
    (:meta :name "viewport" :content "width=device-width, initial-scale=1")
    (:title title)
    (:link :rel "stylesheet" :href *bootstrap*)
    (:link :rel "stylesheet" :href *bootstrap-theme*)
    (:link :rel "stylesheet" :href "/styles"))))

(defun body-head (script)
  (markup
   (:script :type "text/javascript" :src *jquery* nil)
   (:script :type "text/javascript" :src *angularjs* nil)
   (:script :type "text/javascript" :src *ui-bootstrap* nil)
   (:script :type "text/javascript" :src script nil)))

(defun ax (expression)
  (format nil "{{~a}}" expression))

(defun fetch-affinity (reference-wid wid)
  (db-cmd *db* :query
          (:select 'aid
           :from *affinity-table*
           :where (:and (:= 'aid reference-wid)
                        (:= 'bid wid)))))

(defun insert-affinity (reference-wid wid score)
  (unless (fetch-affinity reference-wid wid)
    (db-cmd *db* :execute
            (:insert-into *affinity-table*
             :set 'aid reference-wid
                  'bid wid
                  'score score))))
