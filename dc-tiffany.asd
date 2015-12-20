(asdf:defsystem :dc-tiffany

    :depends-on (:cl-ppcre
                 :dc-utilities
                 :cl-markup
                 :lass
                 :parenscript
                 :hunchentoot
                 :do-urlencode
                 :ht-routes
                 :dc-db
                 :postmodern
                 :swank)

    :components ((:file "dc-tiffany")))
