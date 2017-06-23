package metadata

import (
	"encoding/xml"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/fatih/structs"
	"github.com/ghetzel/go-stockutil/stringutil"
)

type nfoActor struct {
	Name  string   `xml:"name" structs:"name"`
	Roles []string `xml:"role" structs:"roles,omitempty"`
	Photo string   `xml:"thumb,omitempty" structs:"photo,omitempty"`
}

type nfoTvShow struct {
	XMLName   struct{}   `xml:"tvshow"`
	Title     string     `xml:"title"`
	Actors    []nfoActor `xml:"actor,omitempty"`
	Genres    []string   `xml:"genre,omitempty"`
	MPAA      string     `xml:"mpaa,omitempty"`
	Plot      string     `xml:"plot,omitempty"`
	Premiered string     `xml:"aired,omitempty"`
	Rating    float64    `xml:"rating,omitempty"`
	Studio    string     `xml:"studio,omitempty"`
}

type nfoEpisodeDetails struct {
	XMLName        struct{}   `xml:"episodedetails"`
	Title          string     `xml:"title"`
	Actors         []nfoActor `xml:"actor,omitempty"`
	Aired          string     `xml:"aired,omitempty"`
	Director       string     `xml:"director,omitempty"`
	DisplayEpisode string     `xml:"displayepisode,omitempty"`
	DisplaySeason  string     `xml:"displayseason,omitempty"`
	Episode        int        `xml:"episode"`
	ID             int        `xml:"id,omitempty"`
	Plot           string     `xml:"plot,omitempty"`
	Rating         float64    `xml:"rating,omitempty"`
	Runtime        int        `xml:"runtime,omitempty"`
	Season         int        `xml:"season"`
	ShowTitle      string     `xml:"showtitle,omitempty"`
	Thumbnail      string     `xml:"thumb,omitempty"`
	Watched        bool       `xml:"watched,omitempty"`
}

type nfoMovieDetails struct {
	XMLName       struct{}   `xml:"movie"`
	Title         string     `xml:"title"`
	Actors        []nfoActor `xml:"actor,omitempty"`
	Genres        []string   `xml:"genre,omitempty"`
	ID            int        `xml:"id,omitempty"`
	MPAA          string     `xml:"mpaa,omitempty"`
	OriginalTitle string     `xml:"originaltitle,omitempty"`
	Plot          string     `xml:"plot,omitempty"`
	Premiered     string     `xml:"aired,omitempty"`
	Tagline       string     `xml:"tagline"`
	Director      string     `xml:"director,omitempty"`
}

type MediaLoader struct {
	Loader
	nfoFileName string
}

func (self *MediaLoader) CanHandle(name string) Loader {
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

	if ext != `.nfo` {
		return path.Join(dir, strings.TrimSuffix(base, ext)+`.nfo`)
	}

	return ``
}

func (self *MediaLoader) parseMediaInfoFile(name string) (map[string]interface{}, error) {
	if file, err := os.Open(name); err == nil {
		decoder := xml.NewDecoder(file)
		rv := make(map[string]interface{})

		// try episodedetails
		// ----------------------------------------------------------------------------------------
		ep := nfoEpisodeDetails{}
		var st *structs.Struct

		if err := decoder.Decode(&ep); err == nil && ep.Title != `` {
			rv[`type`] = `episode`
			st = structs.New(ep)
		}

		// try movie
		// ----------------------------------------------------------------------------------------
		movie := nfoMovieDetails{}

		if err := decoder.Decode(&movie); err == nil && movie.Title != `` {
			rv[`type`] = `movie`
			st = structs.New(movie)
		}

		// try tvshow
		// ----------------------------------------------------------------------------------------
		show := nfoTvShow{}

		if err := decoder.Decode(&show); err == nil && show.Title != `` {
			rv[`type`] = `tvshow`
			st = structs.New(show)
		}

		if st != nil {
			for _, field := range st.Fields() {
				if !field.IsZero() {
					rv[stringutil.Underscore(field.Name())] = field.Value()
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
}
