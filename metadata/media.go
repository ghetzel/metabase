package metadata

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/fatih/structs"
	"github.com/ghetzel/go-stockutil/stringutil"
)

type nfoActor struct {
	Name  string   `json:"name"            xml:"name"            structs:"name"`
	Roles []string `json:"roles"           xml:"role"            structs:"roles,omitempty"`
	Photo string   `json:"photo,omitempty" xml:"thumb,omitempty" structs:"photo,omitempty"`
}

type nfoTvShow struct {
	XMLName   xml.Name   `xml:"tvshow"`
	Title     string     `json:"title"               xml:"title"`
	Actors    []nfoActor `json:"actors,omitempty"    xml:"actor,omitempty"`
	Genres    []string   `json:"genres,omitempty"    xml:"genre,omitempty"`
	MPAA      string     `json:"mpaa,omitempty"      xml:"mpaa,omitempty"`
	Plot      string     `json:"plot,omitempty"      xml:"plot,omitempty"`
	Premiered string     `json:"premiered,omitempty" xml:"premiered,omitempty"`
	Rating    float64    `json:"rating,omitempty"    xml:"rating,omitempty"`
	Studio    string     `json:"studio,omitempty"    xml:"studio,omitempty"`
}

type nfoEpisodeDetails struct {
	XMLName        xml.Name   `xml:"episodedetails"`
	Title          string     `json:"title"                    xml:"title"`
	Actors         []nfoActor `json:"actors,omitempty"         xml:"actor,omitempty"`
	Aired          string     `json:"aired,omitempty"          xml:"aired,omitempty"`
	Director       string     `json:"director,omitempty"       xml:"director,omitempty"`
	DisplayEpisode string     `json:"displayepisode,omitempty" xml:"displayepisode,omitempty"`
	DisplaySeason  string     `json:"displayseason,omitempty"  xml:"displayseason,omitempty"`
	Episode        int        `json:"episode"                  xml:"episode"`
	ID             int        `json:"id,omitempty"             xml:"id,omitempty"`
	Plot           string     `json:"plot,omitempty"           xml:"plot,omitempty"`
	Rating         float64    `json:"rating,omitempty"         xml:"rating,omitempty"`
	Runtime        int        `json:"runtime,omitempty"        xml:"runtime,omitempty"`
	Season         int        `json:"season"                   xml:"season"`
	ShowTitle      string     `json:"showtitle,omitempty"      xml:"showtitle,omitempty"`
	Thumbnail      string     `json:"thumb,omitempty"          xml:"thumb,omitempty"`
	Watched        bool       `json:"watched,omitempty"        xml:"watched,omitempty"`
}

type nfoMovieDetails struct {
	XMLName       xml.Name   `xml:"movie"`
	Title         string     `json:"title"                   xml:"title"`
	Actors        []nfoActor `json:"actors,omitempty"        xml:"actor,omitempty"`
	Genres        []string   `json:"genres,omitempty"        xml:"genre,omitempty"`
	ID            int        `json:"id,omitempty"            xml:"id,omitempty"`
	MPAA          string     `json:"mpaa,omitempty"          xml:"mpaa,omitempty"`
	OriginalTitle string     `json:"originaltitle,omitempty" xml:"originaltitle,omitempty"`
	Plot          string     `json:"plot,omitempty"          xml:"plot,omitempty"`
	Premiered     string     `json:"aired,omitempty"         xml:"aired,omitempty"`
	Tagline       string     `json:"tagline"                 xml:"tagline"`
	Director      string     `json:"director,omitempty"      xml:"director,omitempty"`
}

type MediaLoader struct {
	Loader
	nfoFileName string
}

func (self *MediaLoader) CanHandle(name string) Loader {
	if stat, err := os.Stat(name); err == nil && stat.IsDir() {
		showinfo := path.Join(name, `tvshow.nfo`)

		if _, err := os.Stat(showinfo); err == nil {
			return &MediaLoader{
				nfoFileName: showinfo,
			}
		}
	}

	if nfoFileName := self.getNfoPath(name); nfoFileName != `` {
		if _, err := os.Stat(nfoFileName); err == nil {
			return &MediaLoader{
				nfoFileName: nfoFileName,
			}
		}
	}

	return nil
}

func (self *MediaLoader) LoadMetadata(name string) (map[string]interface{}, error) {
	if self.nfoFileName != `` {
		return self.parseMediaInfoFile(self.nfoFileName)
	}

	return nil, nil
}

func (self *MediaLoader) getNfoPath(name string) string {
	dir, base := path.Split(name)
	ext := path.Ext(base)

	if base == `tvshow.nfo` {
		return name
	}

	if ext != `.nfo` {
		return path.Join(dir, strings.TrimSuffix(base, ext)+`.nfo`)
	}

	return ``
}

func (self *MediaLoader) parseMediaInfoFile(name string) (map[string]interface{}, error) {
	if file, err := os.Open(name); err == nil {
		if data, err := ioutil.ReadAll(file); err == nil {
			rv := make(map[string]interface{})

			// try episodedetails
			// ----------------------------------------------------------------------------------------
			ep := nfoEpisodeDetails{}
			var st *structs.Struct

			if err := xml.Unmarshal(data, &ep); err == nil {
				if ep.Title != `` {
					// include the parent tvshow details (if available)
					if showfile := path.Join(path.Dir(name), `tvshow.nfo`); showfile != name {
						if tvshow, err := self.parseMediaInfoFile(showfile); err == nil {
							if info, ok := tvshow[`media`]; ok {
								rv[`show`] = info
							}
						}
					}

					rv[`type`] = `episode`
					st = structs.New(ep)
				}
			}

			// try movie
			// ----------------------------------------------------------------------------------------
			movie := nfoMovieDetails{}

			if err := xml.Unmarshal(data, &movie); err == nil {
				if movie.Title != `` {
					rv[`type`] = `movie`
					st = structs.New(movie)
				}
			}

			// try tvshow
			// ----------------------------------------------------------------------------------------
			show := nfoTvShow{}

			if err := xml.Unmarshal(data, &show); err == nil {
				if show.Title != `` {
					rv[`type`] = `tvshow`
					st = structs.New(show)
				}
			}

			if st != nil {
				for _, field := range st.Fields() {
					if !field.IsZero() {
						key := stringutil.Underscore(field.Name())

						switch key {
						case `xml_name`:
							continue
						default:
							rv[key] = field.Value()
						}
					}
				}

				return map[string]interface{}{
					`media`: rv,
				}, nil
			}

			return nil, fmt.Errorf("Unrecognized MediaInfo file format at %q", name)
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}
